#!/usr/bin/python
"""Functional modules to batch, distribute, and compile dependency matrix generation.

NOTE: All computations are between rows of matrix. (row vectors)
"""
import numpy as np
from compute_dependencies.computers import COMPUTERS
from compute_dependencies import *
from py_symmetric_matrix import *
from util import *
import json
import os


RX_SELF_BATCHNAME = re.compile("(?P<fname>[^_]+)_(?P<start>\d+)_(?P<end>\d+)_self")
RX_DUAL_BATCHNAME = re.compile("(?P<fname1>[^_]+)_(?P<fname2>[^_]+)_(?P<offset>[^_]+)_dual")

BATCH_SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "batch_script.py")
COMPILE_SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "compile_script.py")
JSONINDEX_SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jsonindex_script.py")

def shell_compile(**kwds):
  """Compile directory of dependency matrix fragments.

  EXAMPLE:
    python $HOME/dependency_matrix/compile_script.py compile_dir=/fs/lustre/osu6683/gse15745_nov2/dependency_dispatch/PCC outdir=/fs/lustre/osu6683/gse15745_nov2/testPCC_compile n_rows=24334 n_cols=10277 mtype=dual
  """
  assert len(set(('compile_dir', 'outdir', 'n_rows', 'n_cols', 'mtype')) & set(kwds.keys())) == len(kwds)
  args = ["%s=%s"%(k,v) for k, v in kwds.items() if v is not None]
  return "python %s %s" % (COMPILE_SCRIPT_PATH, " ".join(args))

def jsonindex_outname(exelog_fname):
  return exelog_fname.rpartition('.')[0]+'.json'

def shell_jsonindex(*args, **kwds):
  assert 'exelog_fname' in kwds
  args = ["%s=%s"%(k,v) for k, v in kwds.items() if v is not None]
  return "python %s %s" % (JSONINDEX_SCRIPT_PATH, " ".join(args))

def shell_batch(compute_options=None, **kwds):
  """Return batch_script.py shell with parameters.

  Returns:
    str of shell command
  """
  args = ["%s=%s"%(k,v) for k, v in kwds.items() if v is not None]
  if compute_options:
    args.append('compute_options="%s"' % json.dumps(compute_options).replace('"','\"'))
  return "python %s %s" % (BATCH_SCRIPT_PATH, " ".join(args))

def shells_dispatch_self(fname=None, n=None, computer=None, compute_options=None, k=100000, outdir=None):
  """Return batch_script.py shells to compute all-pairs for one matrix.

  Returns:
    [str] of shell commands
  """
  assert fname and n > 0 and computer
  if compute_options is None: compute_options = {}
  nn = n*(n-1)/2
  n_batches = k//nn+1
  batches = []
  for i in xrange(n_batches):
    start = i*k
    end = min(start+k, nn)
    line = shell_batch(fname=fname, start=start, end=end, computer=computer, compute_options=compute_options, outdir=outdir)
    batches.append(line)
  return batches

def shells_dispatch_dual(fname1=None, fname2=None, n1=None, computer=None, compute_options=None, outdir=None):
  """Return batch_script.py shells to compute all-pairs between two matrices.

  Returns:
    [str] of shell commands
  """
  assert fname1 and fname2 and n1 > 0 and computer
  if compute_options is None: compute_options = {}
  batches = []
  # loop over each ROW of M1
  for i in xrange(n1): 
    cmd = shell_batch(fname1=fname1, fname2=fname2, offset=i, computer=computer, compute_options=compute_options, outdir=outdir)
    batches.append(cmd)
  return batches

def get_computer(computer, size, compute_options=None):
  if compute_options is None:
    compute_options = {}
  C = BatchComputer(COMPUTERS[computer](**compute_options), size)
  return C
  
def compute_self(M=None, C=None, start=0, size=1, verbose=False):
  """Compute batch of all-pairs (upper triangle) dependency matrix in one matrix.

  Returns:
    BatchComputer: obj all computed dependencies in this batch
    or None if overwrite is True and all files that would have been saved exist
  """
  assert C is not None and M is not None
  n = np.size(M,0)
  for i in xrange(size):
    xi, yi = inv_sym_idx(start+i, n)
    assert xi < n and yi < n and xi < yi and sym_idx(xi, yi, n) == start+i
    x, y = intersect(M[xi,:], M[yi,:])
    v = C.compute(x, y, i)
    if verbose:
      print i, v
  return C

def compute_dual(M1=None, M2=None, C=None, offset=None, verbose=False):
  """Compute batch of all-pairs (rectangular) dependency matrix between two matrices.
  rows of M1 will correspond to ROWS of dependency matrix
  rows of M2 will correspond to COLUMNS of dependency matrix
  `offset` corresponds to row in M1 and thus row in final dependency matrix

  Returns:
    BatchComputer: obj all computed dependencies in this batch
    or None if overwrite is True and all files that would have been saved exist.
  """
  assert C is not None and M1 is not None and M2 is not None
  assert np.size(M1,1) == np.size(M2,1) # number of columns (samples) must be equal
  n1, n2 = np.size(M1,0), np.size(M2,0)
  assert 0 <= offset and offset < n1
  # i corresponds to row in M2 and column per row `offset` in dependency matrix
  for i in xrange(n2):
    x, y = intersect(M1[offset,:], M2[i,:])
    v = C.compute(x, y, i)
    if verbose:
      print i, v
  return C


class CompiledMatrix(object):
  def __init__(self, n, dtype=np.float):
    assert n > 0
    self.M = np.zeros(n, dtype=dtype)
    self.B = np.zeros(n, dtype=np.bool)
    self.n_set_total, self.n_dupe_total, self.n_nan_total = 0, 0, 0
    self.Q_last = None

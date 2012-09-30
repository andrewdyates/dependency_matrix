#!/usr/bin/python
import numpy as np
from compute_dependencies.computers import COMPUTERS
from compute_dependencies import BatchComputer
from py_symmetric_matrix import *
import json
import os


BATCH_SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "batch_script.py")

def make_batch(compute_options=None, **kwds):
  args = ["auto"]
  args = ["%s=%s"%(k,v) for k, v in kwds]
  if compute_options:
    args.append('compute_options="%s"' % json.dumps(compute_options).replace('"','\"'))
  return "python %s %s" % (BATCH_SCRIPT_PATH, " ".join(args))

def cmd_dispatch_self(fname=None, n=None, computer=None, compute_options=None, k=200000):
  """Return a list of batch commands to compute all-pairs dependency for one matrix."""
  assert fname and n > 0 and computer
  if compute_options is None: compute_options = {}
  nn = n*(n-1)/2
  n_batches = k//nn+1
  batches = []
  for i in xrange(n_batches):
    start = i*k
    end = min(start+k, nn)
    batches.append(make_batch(fname=fname, start=start, end=end, computer=computer, compute_options=compute_options))
  return batches

def cmd_dispatch_dual(fname1=None, fname2=None, n2=None, computer=None, compute_options=None):
  """Return a list of batch commands to compute all-pairs dependency for two matrices."""
  assert fname1 and fname2 and n2 > 0 and computer
  if compute_options is None: compute_options = {}
  batches = []
  for i in xrange(n2):
    batches.append(make_batch(fname1=fname1, fname2=fname2, offset=i, computer=computer, compute_options=compute_options))
  return batches

def batch_self(M=None, computer=None, start=0, end=None, compute_options=None):
  """Compute batch of all-pairs (upper triangle) dependency matrix in one matrix.

  Returns:
    BatchComputer: obj all computed dependencies in this batch
  """
  if compute_options is None: compute_options = {}
  n = np.size(M,0)
  max_i = n*(n-1)/2
  if end is None:
    end = max_i
  else:
    assert end <= max_i
  size = end-start
  C = BatchComputer(COMPUTERS[computer](**compute_options), size)
  for i in xrange(size):
    xi, yi = inv_sym_idx(start+i, n)
    assert xi < n and yi < n and xi < yi and sym_idx(xi, yi, n) == start+i
    x, y = intersect(M[xi,:], M[yi,:])
    print x, y
    C.compute(x, y, i)
  return C

def batch_dual(M1=None, M2=None, computer=None, offset=None, compute_options=None):
  """Compute batch of all-pairs (rectangular) dependency matrix between two matrices.

  Returns:
    BatchComputer: obj all computed dependencies in this batch
  """
  assert np.size(M1,1) == np.size(M2,1)
  if compute_options is None: compute_options = {}
  
  n1, n2 = np.size(M1,0), np.size(M2,0)
  assert 0 <= offset and offset < n1
  C = BatchComputer(COMPUTERS[computer](**compute_options), n2)
  for i in xrange(n2):
    x, y = intersect(M1[offset,:], M2[i,:])
    C.compute(x, y, i)
  return C

def intersect(x, y):
  """Return x, y such that no dimension has a missing value.

  Returns:
    (x,y) np.array tuple with no missing values or mismatched dimensions
  """
  if hasattr(x, 'mask'):
    x_mask = x.mask
  else:
    x_mask = np.zeros(x.shape, dtype=np.bool)
  if hasattr(y, 'mask'):
    y_mask = y.mask
  else:
    y_mask = np.zeros(y.shape, dtype=np.bool)
  join_select = (~x_mask & ~y_mask)
  if hasattr(x, 'mask'):
    r_x = x[join_select].data
  else:
    r_x = x[join_select]
  if hasattr(y, 'mask'):
    r_y = y[join_select].data
  else:
    r_y = y[join_select]    
  return (r_x, r_y)


#!/usr/bin/python
"""Compute a batch of pairwise-dependencies.

Intended to be called in a parallel computation script.
"""
from compute_dependencies.computers import *
from compute_dependencies import *
from matrix_io import *
from qsub import *
from py_symmetric_matrix import *
import sys, os

def main(kwds):
  """Shell script wrapper."""
  if "compute_options" in kwds:
    kwds["compute_options"] = dict([s.split('=') for s in kwds["compute_options"].split(',')])
  outdir = kwds["outdir"]; del kwds["outdir"]
  assert os.path.exists(outdir) and os.path.isdir(outdir)
  
  # Single matrix
  if "fname" in kwds:
    kwds['start'], kwds['end'] = int(kwds['start']), int(kwds['end'])
    kwds['M'] = load(kwds['fname'])['M']; 
    batchname = "%s_%d_%d" % (kwds['fname'], kwds['start'], kwds['end'])
    del kwds['fname'] 
    C = all_pairs_one(**kwds)
  # Dual matrices
  elif "fname1" in kwds and "fname2" in kwds:
    kwds['offset'] = int(kwds['offset'])
    kwds['M1'], kwds['M2'] = load(kwds['M1'])['M'], load(kwds['M2'])['M']
    batchname = "%s_%s_%d" % (kwds['fname1'], kwds['fname2'], kwds['offset'])
    del kwds['fname1']; del kwds['fname2']
    C = all_pairs_two(**kwds)
  else:
    raise Exception, "Unrecognized batch options: %s" % kwds

  n_nans = C.nans()
  if n_nans > 0:
    print "!WARNING: %d nans in computation batch %s." % (n_nans, batchname)
  outnames = C.save(outdir, batchname)
  print "Saved: %s" % outnames
  return 0

def all_pairs_one(M=None, computer=None, start=None, end=None, compute_options=None):
  """Compute batch of all-pairs (upper triangle) dependency matrix in one matrix."""
  assert 0 <= start and start < end
  assert M is not None
  assert computer in COMPUTERS
  if compute_options is None: compute_options = {}

  size = end-start
  n = np.size(M,0)
  assert end <= n*(n-1)/2
  C = BatchComputer(COMPUTERS[computer](**compute_options), size)
  for i in xrange(size):
    xi, yi = inv_sym_idx(start+i, n)
    assert xi < n and yi < n and xi < yi and sym_idx(xi, yi, n) == start+i
    x, y = intersect(M[xi,:], M[yi,:])
    C.compute(x, y, i)
  return C

def all_pairs_two(M1=None, M2=None, computer=None, offset=None, compute_options=None):
  """Compute batch of all-pairs (rectangular) dependency matrix between two matrices."""
  assert M1 is not None and M2 is not None
  assert computer in COMPUTERS
  if compute_options is None: compute_options = {}
  n1, n2 = np.size(M1,0), np.size(M2,0)
  assert n1 < n2 and offset < n1 and offset >= 0
  C = BatchComputer(COMPUTERS[computer](**compute_options), n2)
  for i in xrange(n2):
    x, y = intersect(M1[offset,:], M2[i,:])
    C.compute(x, y, i)
  return C

def intersect(x, y):
  if hasattr(x, 'mask'):
    x_mask = x.mask
  else:
    x_mask = np.zeros(x.shape, dtype=np.bool)
  if hasattr(y, 'mask'):
    y_mask = y.mask
  else:
    y_mask = np.zeros(y.shape, dtype=np.bool)
  join_mask = ~(~x_mask & ~y_mask)
  return (x[join_mask], y[join_mask])


if __name__ == "__main__":
  kwds = dict([s.split('=') for s in sys.argv[1:]])
  main(kwds)

  

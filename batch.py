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

F = ["single", "dual"]

def all_pairs_one(fname=None, computer=None, start=None, end=None, outdir=None, compute_options=None):
  """Compute batch of all-pairs (upper triangle) dependency matrix in one matrix."""
  start, end = int(start), int(end)
  assert 0 <= start and start < end
  assert computer in COMPUTERS
  assert os.path.exists(outdir) and os.path.isdir(outdir)
  if compute_options is None: compute_options = {}

  size = end-start
  batchname = "%s_%s_%d_%d" % (fname, computer, start, end)
  M = load(fname)["M"]
  n = np.size(M,0)
  assert end <= n*(n-1)/2
  C = BatchComputer(COMPUTERS[computer](**compute_options), size)
  for i in xrange(size):
    xi, yi = inv_sym_idx(start+i, n)
    assert xi < n and yi < n and xi < yi and sym_idx(xi, yi, n) == start+i
    x, y = intersect(M[xi,:], M[yi,:])
    C.compute(x, y, i)
  warn_nans(C, batchname)
  outnames = C.save(outdir, batchname)
  return outnames

def all_pairs_two(fname1=None, fname2=None, computer=None, offset=None, outdir=None, compute_options=None):
  """Compute batch of all-pairs (rectangular) dependency matrix between two matrices."""
  offset = int(offset)
  assert computer in COMPUTERS
  assert os.path.exists(outdir) and os.path.isdir(outdir)
  if compute_options is None: compute_options = {}
  
  M1, M2 = load(fname1)["M"], load(fname2)["M"]
  n1, n2 = np.size(M1,0), np.size(M2,0)
  assert n1 < n2 and offset < n1 and offset >= 0
  batchname = "%s_%s_%d" % (fname1, fname2, offset)
  C = BatchComputer(COMPUTERS[computer](**compute_options), n2)
  for i in xrange(n2):
    x, y = intersect(M1[offset,:], M2[i,:])
    C.compute(x, y, i)
  warn_nans(C, batchname)
  outnames = C.save(outdir, batchname)
  return outnames

def warn_nans(C, batchname):
  n_nans = C.nans()
  if n_nans > 0:
    print "!WARNING: %d nans in computation batch %s." % (n_nans, batchname)
    return True
  else:
    return False

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

def main(kwds):
  f = kwds["f"]; del kwds["f"]
  if f == "single":
    all_pairs_one(**kwds)
  elif f == "dual":
    all_pairs_two(**kwds)
  else:
    raise Exception, "undefined f=%s" % f

if __name__ == "__main__":
  kwds = dict([s.split('=') for s in sys.argv[1:]])
  print kwds
  assert "f" in kwds and kwds["f"] in ["single", "dual"]
  main(kwds)

  

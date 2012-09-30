#!/usr/bin/python
import numpy as np
from matrix_io import *
from qsub import *

BATCH_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "batch.py")


def dispatch_all_pairs_one(fname=None, k=None):
  pass

def dispatch_all_pairs_two(fname1, fname2):
  pass
  
def main(fname):
  ext = fname.rpartition('.')[2].lower()
  M = load(fname)["M"]
  if ext not in ("pkl", "npy"):
    fname = basename+".pkl"
    save(M, fname)

  
#!/usr/bin/python
import qsub

def a():
  n = np.size(M,0)
  nn = n*(n-1)/2
  

def dispatch_one(fname, n, computer, k=200000):
  """Return command line to parallel job execution."""
  nn = n*(n-1)/2
  n_batches = k//nn+1
  batches = []
  for i in xrange(n_batches):
    start = i*k
    end = min(start+k, nn)
    batches.append(make_batch(fname, start, end, computer))
  return batches

def make_batch(fname, start, end, computer):
  assert start < end
  BATCH_PATH
  os.abspath(__file__)
  "python %s" % " ".join(["%s=%s"%(k,str(v)) for k,v in ar])

# load matrix
# for each measure, dispatch to compute


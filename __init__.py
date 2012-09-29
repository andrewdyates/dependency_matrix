#!/usr/bin/python
import numpy as np
import qsub


def a():
  n = np.size(M,0)
  nn = n*(n-1)/2
  

def dispatch(fname, n, computer, k=200000):
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
  os.abspath(__file__)

# load matrix
# for each measure, dispatch to compute


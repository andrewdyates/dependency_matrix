#!/usr/bin/python
from matrix_io import *
from qsub import *


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

  

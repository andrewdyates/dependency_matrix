#!/usr/bin/python
from matrix_io import *
from qsub import *

def main(fname):
  basename,c,ext = fname.rpartition('.')[2].lower()
  M = load(fname)["M"]
  if ext not in ("pkl", "npy"):
    fname = basename+".pkl"
    save(M, fname)

  

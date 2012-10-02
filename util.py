#!/usr/bin/python
import os, errno

def make_dir(outdir):
  try:
    os.makedirs(outdir)
  except OSError, e:
    if e.errno != errno.EEXIST: raise
  return outdir

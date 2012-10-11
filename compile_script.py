#!/usr/bin/python
"""Compile batch dependency matrix fragments into one dependency matrix.

SAMPLE USE:
python compile_script.py compile_dir outdir n_rows n_cols self
"""
#RX_SAVE_PTN = re.compile("(?P<batchname>.+?)\.(?P<mname>[^.])+\.npy")
from compute_dependencies import *

#RX_SELF_BATCHNAME = re.compile("(?P<fname>[^_]+)_(?P<start>\d+)_(?P<end>\d+)_self")
#RX_DUAL_BATCHNAME = re.compile("(?P<fname1>[^_]+)_(?P<fname2>[^_]+)_(?P<offset>[^_]+)_dual")

from __init__ import *
import os, sys
import numpy as np

def main(compile_dir=None, outdir=None, n_rows=None, n_cols=None, mtype=None, dtype=np.float32):
  assert os.path.exists(compile_dir) and os.path.exists(outdir)
  if isinstance(dtype) 
  assert mtype in ("self", "dual")
  if mtype == "self":
    if n_rows:
      print "WARNING: n_cols not used for mtype=self"
  else:
    assert n_cols is not None and n_rows is not None
    if n:
      print "WARNING: n not used for mtype=dual"

  if mtype == "self":
    n = n_rows*n_cols
    RX_BATCH = RX_DUAL_BATCHNAME
  else:
    n = n_rows*(n_rows-1)/2
    RX_BATCH = RX_SELF_BATCHNAME
    
  M = np.zeros(n, dtype=dtype)
  B = np.zeros(n, dtype=np.bool)
  n_set_total, n_dupe_total, n_nan_total = 0, 0, 0


if __name__ == "__main__":
  args = dict([s.split('=') for s in sys.argv[1:]])
  

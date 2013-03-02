#!/usr/bin/python
import json
import numpy as np
from matrix_io import *
import sys

def main(jsons="", out_fname=None):
  """Pass jsons as list of file pathes, comma seperated.
  out is an open writable file pointer
  """
  assert out_fname
  assert jsons
  out = open(out_fname, "w")
  for json_f in jsons.split(','):
    print >> out, "!Load %s..." % json_f
    J = json.load(open(json_f))
    for md in J:
      print >> out, md
      M = load(md['values'])['M']
      print >> out, "np.size(M)", np.size(M)
      print >> out, "M.shape", M.shape
      nan_mask = np.isnan(M)
      print >> out, "np.sum(np.isnan(M))", np.sum(nan_mask)
      print >> out, "masking nans..."
      # There's a bug in np.sum of float32s;
      #   this distorts the mean (but not histogram)
      #   convert to float64 to correct this issue
      print >> out, "Removed %d masked values from consideration." % (np.sum(nan_mask))
      print >> out, "Converted matrix to float64 to mitigate np.sum float32 bug."
      # Remove missing values from consideration
      if 'missing' in md:
        print >> out, "Missing values reported to exist. Loading IsSet matrix %s." % md['missing']
        B = load(md['missing'])['M']
        assert B.shape == M.shape and B.size == M.size
        n_missing = np.sum(B==0)
        print >> out, "%d missing values, %d existing values, %d total values." % (n_missing, B.size-n_missing, B.size)
        isset_mask = np.array(B)
        mask = nan_mask|(~isset_mask)
        print >> out, "%d total masked values (%d nans, %d missing)" % (np.sum(mask), np.sum(nan_mask), np.sum(~isset_mask))
      else:
        mask = nan_mask
      Q=np.array(M[~mask], dtype=np.float)
      print >> out, "np.sum(Q)", np.sum(Q)
      print >> out, "np.size(Q)", np.size(Q)
      print >> out, "np.max(Q)", np.max(Q)
      print >> out, "np.min(Q)", np.min(Q)
      print >> out, "np.std(Q)", np.std(Q)
      print >> out, "np.mean(Q)", np.mean(Q)
      print >> out, "np.median(Q)", np.median(Q)
      h = np.histogram(Q)
      print >> out, "Histogram:"
      print >> out, "counts: ", ",".join(map(str, h[0]))
      print >> out, "bins: ", ",".join(map(str, h[1]))
      print >> out, "==="

if __name__ == "__main__":
  argv = dict([s.split('=') for s in sys.argv[1:]])
  print argv
  main(**argv)

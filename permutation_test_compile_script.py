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
      mask = np.isnan(M)
      print >> out, "np.sum(np.isnan(M))", np.sum(mask)
      print >> out, "masking nans..."
      # There's a bug in np.sum of float32s;
      #   this distorts the mean (but not histogram)
      #   convert to float64 to correct this issue
      print >> out, "Removed %d masked values from consideration; converted matrix to float64 to mitigate np.sum float32 bug." % (np.sum(mask))
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
      print >> out, "bins: ", ",".join(map(str, h[0]))
      print >> out, "counts: ", ",".join(map(str, h[1]))
      print >> out, "==="

if __name__ == "__main__":
  argv = dict([s.split('=') for s in sys.argv[1:]])
  print argv
  main(**argv)

#!/usr/bin/python
import json
from pkl_txt_RData import pkl_to_RData_script
import sys

def main(fname=None, outdir=None, row_fname=None, col_fname=None):
  assert fname
  assert row_fname or col_fname
  if outdir is None:
    outdir = os.path.dirname(fname)
  J = json.load(open(fname))
  
  for d in J:
    print pkl_to_RData_script.main(pkl_fname=d['values'], row_fname=row_fname, col_fname=col_fname, outdir=outdir)

if __name__ == "__main__":
  args = dict([s.split('=') for s in sys.argv[1:]])
  print args
  main(**args)
    
    

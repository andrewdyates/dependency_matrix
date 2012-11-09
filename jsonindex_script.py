#!/usr/bin/python
"""Convert simple per-line log to JSON file. Delete old log."""
from __init__ import *
import os, sys

def main(exelog_fname=None):
  assert exelog_fname is not None
  exelog_fname_json = jsonindex_outname(exelog_fname)
  print "Converting txt file %s to JSON %s..." % (exelog_fname, exelog_fname_json)
  fp = open(exelog_fname_json, 'w')
  lines = ["{%s}"%line.rstrip('\n\r') for line in open(exelog_fname)]
  fp.write("[\n")
  fp.write(',\n'.join(lines))
  fp.write("\n]\n")
  fp.close()
  print "Deleting %s ..." % (exelog_fname)
  os.remove(exelog_fname)
  return exelog_fname_json

if __name__ == "__main__":
  args = dict([s.split('=') for s in sys.argv[1:]])
  print args
  main(**args)

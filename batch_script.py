#!/usr/bin/python
from __init__ import *
from compute_dependencies.computers import *
from compute_dependencies import *
import argparse
import json
from matrix_io import *
import os, sys, errno


def main(computer=None, compute_options=None, outdir=None, fname=None, start=None, end=None, fname1=None, fname2=None, offset=None):
  """Shell script wrapper."""
  assert computer in COMPUTERS.keys()
  if compute_options is not None:
    compute_options =json.loads(compute_options)
  if outdir is None:
    outdir = os.getcwd()
  if not os.path.exists(outdir):
    make_dir(outdir)
  assert os.path.exists(outdir) and os.path.isdir(outdir)
  assert bool(fname) != (fname1 and fname2)
  
  # Single matrix
  if fname:
    M = load(fname)["M"]
    n = np.size(M,0)
    if start is None:
      start = 0
    else:
      start = int(start)
    if end is None:
      end = n*(n-1)/2
    else:
      end = int(end)
    batchname = "%s_%d_%d" % (fname, start, end)
    C = batch_self(M, computer, start, end, compute_options)
  # Dual matrices
  else:
    M1, M2 = load(fname1)["M"], load(fname2)["M"]
    if offset is not None:
      offset = int(offset)
      batchname = "%s_%s_%d" % (fname1, fname2, offset)
    else:
      batchname = "%s_%s_all"
    C = batch_dual(M1, M2, computer, offset, compute_options)

  n_nans = C.nans()
  if n_nans > 0:
    print "!WARNING: %d nans in computation batch %s." % (n_nans, batchname)
  outnames = C.save(outdir, batchname)
  print "Computed %d pairs." % C.n_computed
  print "Saved: %s" % outnames
  

class JSONAction(argparse.Action):
  def __call__(self, parser, namespace, values, option_string=None):
    assert True or parser is None or option_string is None
    setattr(namespace, self.dest, json.loads(values))

def arg_parse_call():
  """Handle command line options, execute program."""
  # Parent subprocessor (for shared functionality). These options come first.
  ps = argparse.ArgumentParser(add_help=False)
  ps.add_argument("computer", choices=COMPUTERS.keys(), help="Computer name to compute dependency measure.")
  ps.add_argument("-o", "--outdir", help="Path to output directory for results. [defaults to current working directory]")
  ps.add_argument("-c", "--compute_options", action=JSONAction, help="JSON formatted dict of computer-specific options.")
  # Global parser
  parser = argparse.ArgumentParser(description="Compute all-pairs dependency between pairs of row vectors.")
  subparsers = parser.add_subparsers(title="Action", help="type: `cmd [option] -h` for method-specific options")
  # Auto subparser
  p_auto = subparsers.add_parser('auto', help="Manually specify main() parameters in key=value format.")
  p_auto.set_defaults(action="auto")
  # Self subparser
  p_self = subparsers.add_parser('self', parents=[ps], help="Compute (n choose 2) all-pairs for a single matrix. Not specifying start and end indices will compute the entire dependency matrix.")
  p_self.add_argument("fname", help="File path to matrix to load")
  p_self.add_argument("-s", "--start", default=0, help="Start index between 0 and (n choose 2)-1  [DEFAULT: 0]", type=int)
  p_self.add_argument("-e", "--end", help="End index between 1 and (n choose 2) [DEFAULT: (n choose 2)]", type=int)
  p_self.set_defaults(action="self")
  # Dual subparser
  p_dual = subparsers.add_parser('dual', parents=[ps], help="Compute (n by m) all-pairs between two matrices. Not specifying offset index will compute the entire dependency matrix.")
  p_dual.add_argument("fname1", help="File path to matrix 1 to load")
  p_dual.add_argument("fname2", help="File path to matrix 2 to load")
  p_dual.add_argument("-s" "--offset", help="Row (indexed from 0) in matrix 1 against with which to compute against all rows in matrix 2 [DEFAULT: no offset, compute entire matrix]", type=int)
  p_self.set_defaults(action="dual")

  kwds = vars(parser.parse_args())
  print "Program arguments: ", kwds

  # Override command line handling, use sys.argv dictionary format directly.
  if kwds['action'] == "auto":
    main(**dict([s.split('=') for s in sys.argv[2:]]))
  else:
    del kwds['action']
    main(**kwds)

def make_dir(outdir):
  try:
    os.makedirs(outdir)
  except OSError, e:
    if e.errno != errno.EEXIST: raise
  return outdir

  
if __name__ == "__main__":
  arg_parse_call()

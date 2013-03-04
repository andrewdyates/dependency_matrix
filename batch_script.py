#!/usr/bin/python
"""Manual script for computing all-pairs dependency matrix batches.

Saves files in outdir in pattern:

    [outdir]/[batchname].[computer.MNAME].npy
    
    where [batchname] matches
      self: RX_SELF_BATCHNAME
        [matrix_fname]_[start]_[end]_self
      dual: RX_DUAL_BATCHNAME
        [matrix_fname_1]_[matrix_fname_2]_[offset]_dual

    EXAMPLE SAVED FILE:
      /path/outdir/GSE15745-GPL6104.data.aligned.pkl_GSE15745-GPL8490.data.aligned.pkl_0_dual.STD_PRODUCT.npy

EXAMPLE USE:
time python /nfs/01/osu6683/recomb2013_workbench/dependency_matrix/batch_script.py computer=PCC fname2=/fs/lustre/osu6683/recomb2013_gse15745/dispatch_test/GSE15745_GPL8490.data.aligned.pkl fname1=/fs/lustre/osu6683/recomb2013_gse15745/dispatch_test/GSE15745_GPL6104.data.aligned.pkl offset=0 verbose=True
"""
from __init__ import *
from compute_dependencies.computers import *
from compute_dependencies import *
import json
from matrix_io import *
import os, sys, errno


def fclean(fname):
  return os.path.basename(fname).replace("_","-")

def main(computer=None, compute_options=None, outdir=None, fname=None, start=None, end=None, fname1=None, fname2=None, offset=0, verbose=False, overwrite=False):
  """Shell script wrapper."""
  assert computer in COMPUTERS.keys()
  if compute_options is not None:
    compute_options = json.loads(compute_options)
  if outdir is None:
    outdir = os.getcwd()
  if not os.path.exists(outdir):
    make_dir(outdir)
  assert os.path.exists(outdir) and os.path.isdir(outdir)
  assert bool(fname) != bool(fname1 and fname2)
  if fname is not None:
    mtype = "self"
  else:
    mtype = "dual"

  # Single matrix
  if mtype == "self":
    M = load(fname)["M"]
    n = np.size(M,0)
    if start is None:
      start = 0
    else:
      start = int(start)
    if end is None:
      end = int(n*(n-1)/2)
    else:
      s = end.rpartition('.')
      if s[0]:
        end = int(s[0])
      else:
        end = int(s[2])
    batchname = "%s_%d_%d_self" % (fclean(fname), start, end)
    block_size = end-start
  # Dual matrices
  else:
    M1, M2 = load(fname1)["M"], load(fname2)["M"]
    if offset is not None:
      offset = int(offset)
      batchname = "%s_%s_%d_dual" % (fclean(fname1), fclean(fname2), offset)
    else:
      batchname = "%s_%s_all_dual"
    block_size = np.size(M2, 0)
    
  # Construct BatchComputer object
  C = get_computer(computer, block_size, compute_options)
  # Check for overwrites
  if not overwrite:
    fnames = C.get_save_names(outdir, batchname).values()
    if all([os.path.exists(s) for s in fnames]):
      print "EXITING WITHOUT COMPUTATION: All files to be written exist, and overwrite flag is false:"
      print ", ".join(fnames)
      return
  
  # Run computation
  if mtype == "self":
    compute_self(M, C, start, block_size, verbose)
  else:
    compute_dual(M1, M2, C, offset, verbose)

  if verbose:
    print batchname
    print "match rx self?", RX_SELF_BATCHNAME.match(batchname)
    print "match rx dual?", RX_DUAL_BATCHNAME.match(batchname)
  assert RX_SELF_BATCHNAME.match(batchname) != RX_DUAL_BATCHNAME.match(batchname)
  n_nans = C.nans()
  if n_nans > 0:
    print "!WARNING: %d nans in computation batch %s." % (n_nans, batchname)
  outnames = C.save(outdir, batchname, overwrite)
  print "Computed %d pairs over %d compute measures." % (C.n_computed, len(C.MNAMES))
  print "Saved: %s" % outnames

  
if __name__ == "__main__":
  kwds = dict([s.split('=') for s in sys.argv[1:]])
  print kwds
  main(**kwds)




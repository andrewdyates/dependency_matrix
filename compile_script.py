#!/usr/bin/python
"""Compile batch dependency matrix fragments into one dependency matrix.

SAMPLE USE:
DUAL
python $HOME/dependency_matrix/compile_script.py compile_dir=/fs/lustre/osu6683/gse15745_nov2/dependency_dispatch/PCC outdir=/fs/lustre/osu6683/gse15745_nov2/testPCC_compile n_rows=24334 n_cols=10277 mtype=dual
  
> dim(meth.M.Data.aligned)
[1] 24334   420
> dim(mrna.M.Data.aligned)
[1] 10277   420
"""
# filename RX:
#   from compute_dependencies/__init__.py:
#   RX_SAVE_PTN = re.compile("(?P<batchname>.+?)\.(?P<mname>[^.])+\.npy")
# batchname RX:
#   from __init__.py
#   RX_SELF_BATCHNAME = re.compile("(?P<fname>[^_]+)_(?P<start>\d+)_(?P<end>\d+)_self")
#   RX_DUAL_BATCHNAME = re.compile("(?P<fname1>[^_]+)_(?P<fname2>[^_]+)_(?P<offset>[^_]+)_dual")

from compute_dependencies import *
from __init__ import *
import os, sys
import cPickle as pickle
import numpy as np


def main(compile_dir=None, outdir=None, n_rows=None, n_cols=None, mtype="self", dtype=np.float32):
  assert os.path.exists(compile_dir) 
  if isinstance(dtype, int):
    if dtype == "32":
      dtype = np.float32
    elif dtype == "64":
      dtype = np.float64
    else:
      print "Unknown dtype precision:", dtype
      sys.exit(1)
  assert mtype in ("self", "dual")
  if mtype == "self":
    if n_cols:
      print "WARNING: n_cols not used for mtype=self"
    assert n_rows is not None
    n_rows = int(n_rows)
  else:
    assert n_cols is not None and n_rows is not None
    n_cols, n_rows = int(n_cols), int(n_rows)
  if not os.path.exists(outdir):
    make_dir(outdir)
    print "Created outdir %s" % outdir

  if mtype == "self":
    n = n_rows*(n_rows-1)/2
  else:
    n = n_rows*n_cols
  print "Loading %d entries for %s type dependency matrix." % (n, mtype)

  # We don't know in advance how many types of matrices we will need to fill.
  Results = {}
  batch_fname = None
  for fname in sorted(os.listdir(compile_dir)):
    m = RX_SAVE_PTN.match(fname)
    if not m: continue
    batch_name = m.group('batchname')
    matrix_name = m.group('mname')
    if matrix_name not in Results:
      print "Creating new Result matrix '%s' of size %d, type %s, %s form." \
          % (matrix_name, n, str(dtype), mtype)
      Results[matrix_name] = CompiledMatrix(n=n, dtype=dtype)
    R = Results[matrix_name]
      
    # Get CompiledMatrix index depending on matrix type
    if mtype == "self":
      m = RX_SELF_BATCHNAME.match(batch_name)
      start = int(m.group('start'))
      end = int(m.group('end'))
      this_batch_fname = m.group('fname')
    else:
      m = RX_DUAL_BATCHNAME.match(batch_name)
      start = int(m.group('offset')) * n_cols
      end = start + n_cols
      this_batch_fname = "%s_%s" % (m.group('fname1'), m.group('fname2'))
      
    if batch_fname is None:
      batch_fname = this_batch_fname
    else:
      if batch_fname != this_batch_fname:
        print "WARNING! batch_fname's in compile_dir %s do not match. Expected batch_fname: '%s', got '%s'. SKIPPING %s !" \
            % (compile_dir, batch_fname, m.group('fname'), fname)
        continue

    ext = fname.rpartition('.')[2]
    fpath = os.path.join(compile_dir, fname)
    if ext == "npy":
      Q = np.load(fpath)
    elif ext == "pkl":
      Q = pickle.load(open(os.path.join(compile_dir, fname)))
    else:
      print "Unknown file extension '%s' of file path %s. Cannot load. Exiting." % (ext, fpath)
      sys.exit(1)
    # Check to make sure that Q is not the same as last Q for this Result matrix.
    if R.Q_last is not None:
      try:
        assert np.size(Q) != np.size(R.Q_last) or np.abs(np.sum(Q - R.Q_last)) > 0
      except AssertionError:
        print "Matrix segment seems repeated..."
        print fname, Q[:5], R.Q_last[:5], (Q-R.Q_last)[:5], np.sum(Q-R.Q_last)
        raise

    # Populate CompiledMatrix with this matrix segment
    n_set, n_dupe, n_nan = 0, 0, 0
    assert end-start == len(Q), "Q is wrong size. Expect %d, got %d." % \
        (len(Q), end-start)
    for i, x in enumerate(xrange(start, end)):
      R.M[x] = Q[i]
      if np.isnan(Q[i]):
        # Implicit: B[x] = 0
        n_nan += 1
      elif not R.B[x]:
        R.B[x] = True
        n_set += 1
      else:
        n_dupe += 1
        
    # Cache this matrix segment for next iteration's segment dupe check
    R.Q_last = Q
    print "Set %d (%d dupes, %d nan) from %s. Expected %d." % \
        (n_set, n_dupe, n_nan, fname, end-start)
    R.n_set_total += n_set
    R.n_dupe_total += n_dupe
    R.n_nan_total += n_nan

  # Save CompiledMatrix for each result.
  for matrix_name, R in Results.items():
    print "Saving CompiledMatrix for matrix %s..." % (matrix_name)
    print "%.2f%% Matrix Compilation. Set %d (%d dupes, %d nan, %d unmasked) from %s. Expected %d." % \
        (R.n_set_total/n*100, R.n_set_total, R.n_dupe_total, R.n_nan_total, np.sum(R.B), compile_dir, n)
    M_fname = "%s.%s.values.pkl" % (batch_fname, matrix_name)
    B_fname = "%s.%s.isset.pkl" % (batch_fname, matrix_name)

    if mtype == "self":
      pickle.dump(R.M, open(M_fname, "w"), protocol=-1)
    else:
      print "Reshaped matrix from %d vector to (%d, %d) matrix. " % (n, n_rows, n_cols)
      pickle.dump(R.M.reshape(n_rows, n_cols), open(M_fname, "w"), protocol=-1)

    print "Saved %s." % (M_fname)
    if R.n_set_total != n:
      pickle.dump(R.B, open(B_fname, "w"), protocol=-1)
      print "!!! Because values are missing, saved %s." % (B_fname)
    else:
      print "No values missing; did not save boolean 'isset' matrix."

  print "Compilation of %s complete. Saved %d result matrices." % (compile_dir, len(Results))


  
if __name__ == "__main__":
  args = dict([s.split('=') for s in sys.argv[1:]])
  print args
  main(**args)
  

#!/usr/bin/python
"""Permute rows of matrix before dispatching. On computation completion, generate report.
"""
import random
import numpy as np
from __init__ import *
from qsub import *
from matrix_io import *
import datetime
import dispatch_script

def permute_rows(M, seed=True):
  """Randomly permute values in rows of M. Preserve masks. Permutes in place."""
  if seed:
    random.seed()
  for i in xrange(np.size(M,0)):
    try:
      M.mask
    except AttributeError:
      random.shuffle(M[i,:])
    else:
      n_masked = np.sum(M[i,:].mask)
      row = M[i,:].compressed()
      assert np.sum(np.isnan(row)) == 0
      row = list(row)
      row.extend([np.nan]*n_masked)
      random.shuffle(row)
      M[i,:] = row
      M[i,:].mask = np.isnan(row)

# NOTE: n_permutes is number of matrix permutes. Total permutations = #rows_1*#row_2*n_permutes
def main(n_permutes=1, fname=None, fname1=None, fname2=None, computers=None, outdir=None, n_nodes=1, n_ppn=12, hours=8, compute_options=None, dry=False):
  n_permutes = int(n_permutes)
  assert n_permutes >= 1
  if not os.path.exists(outdir):
    make_dir(outdir)
    print "Created outdir %s" % outdir
  n_ppn = int(n_ppn)
  n_nodes = int(n_nodes)
  hours = int(hours)
  assert n_nodes >= 1
  assert n_ppn >= 1 and n_ppn <= 12
  assert hours >= 1 and hours <= 99
  os.chdir(outdir)

  now_timestamp = datetime.datetime.now().isoformat('_')
  out_fname = os.path.join(outdir, "permute%d_report.%s.txt" % (n_permutes, now_timestamp))

  if fname:
    mtype = "self"
    assert os.path.exists(fname)
    M, fname_work = load_cp_and_make_bin(fname, outdir)
    print "Loading single matrix for permutation testing."
  else:
    mtype = "dual"
    assert os.path.exists(fname1) and os.path.exists(fname2)
    M1, fname_work1 = load_cp_and_make_bin(fname1, outdir)
    M2, fname_work2 = load_cp_and_make_bin(fname2, outdir)
    print "Loading two matrices for permutation testing."

  # Dispatch dependency matrices, collect completion pids and execution logs.
  PIDs = []
  JSONs = []
  for i in range(1,n_permutes+1):
    perm_dir = os.path.join(outdir, "permuted_dep_matrix_%d" % i)
    if not os.path.exists(perm_dir):
      make_dir(perm_dir)
      print "Created permutation results directory in outdir: %s" % perm_dir

    ## Dispatch dependency computation
    if mtype == "self":
      print "Permuting %s, iteration %d..." % (fname, i)
      M_p = permute_rows(M)
      perm_fname = bname(fname).rpartition(".")[0] + ".perm%d.pkl"%i
      perm_path = os.path.join(perm_dir, perm_fname)
      print "Saving permutated copy as %s..." % perm_path
      save(M_p, perm_path)
      print "Calling dispatcher..."
      R = dispatch_script.main(fname=perm_path, computers=computers, outdir=perm_dir, n_nodes=n_nodes, n_ppn=n_ppn, hours=hours, compute_options=compute_options, dry=dry)
    else:     # mtype == "dual"
      print "Permuting %s, iteration %d..." % (fname1, i)
      M_p = permute_rows(M1)
      perm_fname = bname(fname1).rpartition(".")[0] + ".perm%d.pkl"%i
      perm_path = os.path.join(perm_dir, perm_fname)
      print "Saving permutated copy as %s..." % perm_path
      save(M_p, perm_path)
      print "Calling dispatcher..."
      R = dispatch_script.main(fname1=perm_path, fname2=fname_work2, computers=computers, outdir=perm_dir, n_nodes=n_nodes, n_ppn=n_ppn, hours=hours, compute_options=compute_options, dry=dry)

    PIDs.append(R['pid'])
    JSONs.append(R['exelog_fname_json'])

  ## Given expected results from dispatcher, dispatch permutation report generator
  ##   to be executed after all dispatched permutations complete.
  if mtype == "self":
    jobname = "PERM_COMP_" + bname(fname)
  else:
    jobname = "PERM_COMP_" + bname(fname1) + ":" + bname(fname2)
    
  Q = Qsub(jobname=jobname, n_nodes=1, n_ppn=12, hours=4, work_dir=outdir, email=True, after_jobids=PIDs)
  cmd = shell_permutation_compile(json_fnames=JSONs, out_fname=out_fname)
  Q.add(cmd)
  pid = Q.submit(dry)
  print "Final permutation PID: %s" % pid
  return pid

      
if __name__ == "__main__":
  main()

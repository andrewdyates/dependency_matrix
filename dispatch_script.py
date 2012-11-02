#!/usr/bin/python
"""Dispatch parallel job to compute dependency matrix.

EXAMPLE USE:
opython $HOME/recomb2013_workbench/dependency_matrix/dispatch_script.py fname1=/fs/lustre/osu6683/recomb2013_gse15745/GSE15745_GPL6104.data.aligned.pkl fname2=/fs/lustre/osu6683/recomb2013_gse15745/GSE15745_GPL8490.data.aligned.pkl computers=[\"Dcor\",\"PCC\"] outdir=/fs/lustre/osu6683/recomb2013_gse15745/dispatch_test dry=True
"""
import datetime
import random
from __init__ import *
from matrix_io import *
from qsub import *
import json
import shutil
import sys


bname = os.path.basename
K = 100000
def load_cp_and_make_bin(fname, outdir):
  d = load(fname)
  M, ftype = d["M"], d["ftype"]
  if ftype != "pkl":
    fname_new = os.path.join(outdir, bname(fname.rpartition('.')[0])+".pkl")
    save(M, fname_new)
    print "Saved binary copy of matrix %s as %s" % (fname, fname_new)
  else:
    fname_new = os.path.join(outdir, bname(fname))
    if os.path.abspath(fname) != os.path.abspath(fname_new) and not os.path.exists(fname_new):
      print "Matrix is not in out directory %s. Copying to %s" % (outdir, fname_new)
      shutil.copy(fname, fname_new)
  return (M, fname_new)


def main(fname=None, fname1=None, fname2=None, computers=None, outdir=None, n_nodes=1, n_ppn=12, hours=8, compute_options=None, dry=False):

  assert bool(fname) != bool(fname1 and fname2)
  if computers is None:
    computers = COMPUTERS.keys()
  else:
    if isinstance(computers, basestring):
      computers = json.loads(computers)
    assert not set(computers) - set(COMPUTERS.keys())
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
  compiled_dir = os.path.join(outdir, "compiled")

  if fname:
    mtype = "self"
    assert os.path.exists(fname)
    M, fname_work = load_cp_and_make_bin(fname, outdir)
    print "Dispatching self dependency matrix computation."
  else:
    mtype = "dual"
    assert os.path.exists(fname1) and os.path.exists(fname2)
    M1, fname_work1 = load_cp_and_make_bin(fname1, outdir)
    M2, fname_work2 = load_cp_and_make_bin(fname2, outdir)
    print "Dispatching dual dependency matrix computation."

  pids = []
  for comp_name in computers:
    comp_dir = os.path.join(outdir, comp_name)
    
    if mtype == "self":
      jobname = "%s_%s" % (comp_name, bname(fname_work))
      Q = Qsub(jobname=jobname, n_nodes=n_nodes, n_ppn=n_ppn, hours=hours, work_dir=outdir)
      cmds = shells_dispatch_self(fname_work, np.size(M,0), comp_name, compute_options, k=K, outdir=comp_dir)
      Q.add_parallel(cmds)
      n_rows = np.size(M,0)
      n_cols = np.size(M,0)
    elif mtype == "dual":
      jobname = "%s_%s_%s" % (comp_name, bname(fname_work1), bname(fname_work2))
      Q = Qsub(jobname=jobname, n_nodes=n_nodes, n_ppn=n_ppn, hours=hours, work_dir=outdir)
      # n1 is the number of rows in matrix 1
      cmds = shells_dispatch_dual(fname_work1, fname_work2, np.size(M1,0), comp_name, compute_options, outdir=comp_dir)
      Q.add_parallel(cmds)
      n_rows = np.size(M1,0)
      n_cols = np.size(M2,0)

    # Submit dispatch job, attached post dependent compilation job
    pid = Q.submit(dry)
    print Q.script()
    print "Submitted dispatch, job ID: %s" % pid
    Q = Qsub(jobname="comp_"+jobname, n_nodes=1, n_ppn=1, hours=4, work_dir=outdir, after_jobids=[pid], email=True)
    cmd = shell_compile(compile_dir=comp_dir, outdir=outdir, n_rows=n_rows, n_cols=n_cols, mtype=mtype)
    Q.add(cmd)
    comp_pid = Q.submit(dry)
    print Q.script()
    print "Submitted compilation for job ID %s, jobname %s, job ID: %s" % (pid, jobname, comp_pid)
    pids.append(comp_pid)

  # After all jobs have completed, compile .json job and send email on completion.
  Q = Qsub(jobname="%s_JSONCompile"%(jobname), work_dir=outdir, email=True, after_jobids=pids)
  Q.add(shell_jsonindex(compiled_dir, comp_dir, mtype))
  print Q.script()
  pid = Q.submit(dry)
  print "Final PID: %s" % pid
  return pid


if __name__ == "__main__":
  kwds = dict([s.split('=') for s in sys.argv[1:]])
  print kwds
  main(**kwds)

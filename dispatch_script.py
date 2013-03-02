#!/usr/bin/python
"""Dispatch parallel job to compute dependency matrix.

EXAMPLE USE:
python $HOME/pymod/dependency_matrix/dispatch_script.py fname1=$HOME/gse15745_aligned_matrices_nov2/Methyl_correct_aligned.tab fname2=$HOME/gse15745_aligned_matrices_nov2/mRNA_correct_aligned.tab computers=[\"Cov\", \"PCC\"] outdir=/fs/lustre/osu6683/gse15745_nov2/dependency_dispatch n_nodes=10 n_ppn=12 hours=10

python $HOME/pymod/dependency_matrix/dispatch_script.py fname1=$HOME/gse15745_nov2012_experiments/gse15745_aligned_matrices_nov2/Methyl_correct_aligned.tab fname2=$HOME/gse15745_nov2012_experiments/gse15745_aligned_matrices_nov2/mRNA_correct_aligned.tab computers=[\"MINE\"] outdir=/fs/lustre/osu6683/gse15745_nov2/dependency_dispatch n_nodes=20 n_ppn=12 hours=48
"""
import datetime
import random
from __init__ import *
from matrix_io import *
from qsub import *
import json
import shutil
import sys
import os

bname = os.path.basename
K = 100000


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
  compiled_dir = os.path.join(outdir, "compiled_dep_matrices")
  
  # Write job submission report in compiled_dir.
  if not os.path.exists(compiled_dir):
    make_dir(compiled_dir)
    print "Created directory for results in outdir: %s" % compiled_dir
  now_timestamp = datetime.datetime.now().isoformat('_')
  run_report_fname = os.path.join(compiled_dir, "run_report_%s.txt" % now_timestamp)
  exelog_fname = os.path.join(compiled_dir, "exe_log_%s.txt" % now_timestamp)
  exelog_fname_json = jsonindex_outname(exelog_fname)
  fp = open(run_report_fname, 'w')
  fp.write("locals: "); fp.write(str(locals())); fp.write('\n')
  fp.close()
  print "Wrote dispatch script execution log to %s." % (run_report_fname)

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
    Q = Qsub(jobname="comp_"+jobname, n_nodes=1, n_ppn=1, hours=4, work_dir=outdir, after_jobids=[pid], email=False)
    cmd = shell_compile(compile_dir=comp_dir, outdir=compiled_dir, n_rows=n_rows, n_cols=n_cols, mtype=mtype, exelog_fp=exelog_fname)
    Q.add(cmd)
    comp_pid = Q.submit(dry)
    print Q.script()
    print "Submitted compilation for job ID %s, jobname %s, job ID: %s" % (pid, jobname, comp_pid)
    pids.append(comp_pid)

  # After all jobs have completed, compile .json job, generate report, and send email on completion.
  Q = Qsub(jobname="%s_JSONCompile"%(jobname), work_dir=outdir, email=False, after_jobids=pids)
  Q.add(shell_jsonindex(exelog_fname))
  print Q.script()
  json_pid = Q.submit(dry)

  report_fname = os.path.join(outdir, "%s_report.%s.txt" % (jobname, now_timestamp))
  Q = Qsub(jobname=jobname, n_nodes=1, n_ppn=12, hours=1, work_dir=outdir, email=True, after_jobids=json_pid)
  cmd = shell_report_dependency_matrix(json_fname=exelog_fname_json, out_fname=report_fname)
  Q.add(cmd)
  print Q.script()
  report_pid = Q.submit(dry)
  
  print "Final execution log file path: %s" % (exelog_fname_json)
  print "Final PID: %s" % report_pid
  print "Final report fname: %s" % report_fname
  return {'pid': report_pid, 'exelog_fname_json': exelog_fname_json, 'report_fname': report_fname}


if __name__ == "__main__":
  kwds = dict([s.split('=') for s in sys.argv[1:]])
  print kwds
  main(**kwds)

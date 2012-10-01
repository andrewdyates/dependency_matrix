#!/usr/bin/python
import datetime
import random


def main(fname=None, computers=None, outdir=None, k=200000, n_nodes=1, n_ppn=1, hours=8):

  # Get computers
  if computers is None:
    computers = COMPUTERS.keys()

  # load matrix
  d = load(fname); M, ftype = d["M"], d["ftype"]
  # get outdir, cd into outdir
  if not os.path.exists(outdir):
    make_dir(outdir)
    os.path.cwd(outdir)
  # save matrix in binary if it was in text format
  if ftype != "pkl":
    fname = os.path.join(outdir, os.path.basename(fname)+".pkl")
    save(M, fname)
  # for each computer
  for comp_name in computers:
    jobname = "%s_%s_%s" % (comp_name, os.path.basename(fname), timestamp)
    Q = Qsub(jobname, n_nodes, n_ppn, hours, work_dir)
    comp_dir = os.path.join(outdir, comp_name)
    cmds = cmd_dispatch_self(fname, np.size(M,0), comp_name, compute_options, k)
    Q.add_parallel(jobs)
    # HOW TO GET FILENAMES SAVED?
    cmd = cmd_compile(comp_dir, COMPUTERS[comp_name].MNAMES)
    # submit qsubs
    
# save json file of expected matrices


#!/usr/bin/python
"""Wrapper to compile .pkl dep matrix result to .RData

python $HOME/pymod/dependency_matrix/compile_to_R_dispatch_wrapper.py fname=$HOME/brca/GSE31448.SCAN.tab computers=[\"PCC\"] outdir=$HOME/test n_nodes=10 n_ppn=12 hours=10
"""
import dispatch_script
import qsub
import os, sys
WRAPPER_FNAME = os.path.join(os.path.dirname(os.path.abspath(__file__)), "compile_to_R_from_json.py")

def main(**kwds):
  R = dispatch_script.main(**kwds)
  pid = R['pid']
  fname = R['exelog_fname_json']
  dry = kwds.get('dry',False)
  # dispatch JSON compiler
  if 'fname' in kwds:
    row_fname = kwds['fname']
    col_fname = kwds['fname']
  elif 'fname1' in kwds and 'fname2' in kwds:
    row_fname = kwds['fname1']
    col_fname = kwds['fname2']
  else:
    raise Exception, "No matrix filenames fname or (fname1, fname2)"
    
  cmd = "time python %(script)s fname=%(fname)s row_fname=%(row_fname)s col_fname=%(col_fname)s outdir=%(outdir)s" % {'script':WRAPPER_FNAME, 'fname':fname, 'row_fname':row_fname, 'col_fname':col_fname, 'outdir':kwds['outdir']}
  Q = qsub.Qsub(hours=24, n_ppn=8, jobname="toR_%s"%pid)
  Q.add(cmd)
  pid = Q.submit(dry)
  print Q.script()
  print "Submitted R-compile script, job ID: %s" % pid
  return pid

if __name__ == "__main__":
  args = dict([s.split('=') for s in sys.argv[1:]])
  print args
  main(**args)

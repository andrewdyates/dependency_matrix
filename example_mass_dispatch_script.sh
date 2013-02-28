#!/bin/bash
## Run dCOR, PCC, COV plus permutation tests on all datasets
ALL_DST=/fs/lustre/osu6683/gse15745_feb27_dep
ALL_SRC=$HOME/gibbs_feb16_cleaned_data
# SAMPLE_SET in (all, crblm, fctx, pons, tctx)
cd $ALL_DST

function run {
METH_ALIGN=$ALL_SRC/gibbs.meth.${SAMPLE_SET}.aligned.psb.corr.feb-15-2013.tab
MRNA_ALIGN=$ALL_SRC/gibbs.mrna.${SAMPLE_SET}.aligned.psb.corr.feb-15-2013.tab
MRNA_ALL=$ALL_SRC/gibbs.mrna.${SAMPLE_SET}.psb.corr.feb-15-2013.tab
METH_ALL=$ALL_SRC/gibbs.meth.${SAMPLE_SET}.psb.corr.feb-15-2013.tab

# mrna x meth
python $HOME/pymod/dependency_matrix/dispatch_script.py fname1=$METH_ALIGN fname2=$MRNA_ALIGN computers=[\"PCC\",\"Cov\",\"Dcor\"] outdir=$ALL_DST/${SAMPLE_SET}_aligned_dual n_nodes=10 n_ppn=12 hours=48
# mrna
python $HOME/pymod/dependency_matrix/dispatch_script.py fname=$MRNA_ALL computers=[\"PCC\",\"Cov\",\"Dcor\"] outdir=$ALL_DST/${SAMPLE_SET}_mrna n_nodes=10 n_ppn=12 hours=48
# meth
python $HOME/pymod/dependency_matrix/dispatch_script.py fname=$METH_ALL computers=[\"PCC\",\"Cov\",\"Dcor\"] outdir=$ALL_DST/${SAMPLE_SET}_meth n_nodes=10 n_ppn=12 hours=48
### Permutation tests
# --------------------
# mrna x meth
python $HOME/pymod/dependency_matrix/permutation_test_dispatch_script.py n_permutes=1 fname1=$METH_ALIGN fname2=$MRNA_ALIGN computers=[\"PCC\",\"Cov\"] outdir=$ALL_DST/${SAMPLE_SET}_aligned_dual_perm n_nodes=10 n_ppn=12 hours=48
# mrna
python $HOME/pymod/dependency_matrix/permutation_test_dispatch_script.py n_permutes=1 fname=$MRNA_ALL computers=[\"PCC\",\"Cov\"] outdir=$ALL_DST/${SAMPLE_SET}_mrna_perm n_nodes=10 n_ppn=12 hours=48
# meth
python $HOME/pymod/dependency_matrix/permutation_test_dispatch_script.py n_permutes=1 fname=$METH_ALL computers=[\"PCC\",\"Cov\"] outdir=$ALL_DST/${SAMPLE_SET}_meth_perm n_nodes=10 n_ppn=12 hours=48
}

# ------------------------------
SAMPLE_SET=all
run
SAMPLE_SET=crblm
run
SAMPLE_SET=fctx
run
SAMPLE_SET=pons
run
SAMPLE_SET=tctx
run

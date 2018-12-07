#!/bin/bash
# This is the starter program and calls the batchup_cp4cds_movefiles.py

odir=/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/lotus-logs/qc-fix-2018-12-07-A/

mkdir -p $odir

for batch in $(seq 0 310); do
    bsub -o $odir/%J.out -W 24:00 ./setup_batch_lotus.sh $batch
done
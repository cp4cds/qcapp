#!/bin/bash
# This is the starter program and calls the batchup_cp4cds_movefiles.py

odir=/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/lotus-logs/va-2020-03-010_r2/

mkdir -p $odir

for batch in $(seq 0 113); do
    bsub -o $odir/%J.out -W 24:00 ./copy_from_archive_setup_batch_lotus.sh $batch
done
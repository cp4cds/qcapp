#!/bin/bash
# This is the starter program and calls the batchup_cp4cds_movefiles.py
odir=lotus-logs/qc_fixes/

mkdir -p $odir

for batch in $(seq 0 50); do
    bsub -o ./$odir/%J.out -W 24:00 ./setup_qcfixes_lotus.sh $batch
done

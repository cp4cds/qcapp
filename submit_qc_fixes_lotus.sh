#!/bin/bash
# This is the starter program and calls the batchup_cp4cds_movefiles.py
odir=lotus-logs/new_datasets_to_publish_2/

mkdir -p $odir

for batch in $(seq 0 50); do
    bsub -o ./$odir/%J.out -W 1:00 ./setup_qcfixes_lotus.sh $batch
done

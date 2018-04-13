#!/bin/bash
# This is the starter program and calls the batchup_cp4cds_movefiles.py
odir=lotus-logs/db-consistency-check/
mkdir -p $odir

for batch in $(seq 0 50); do
    bsub -o ./$odir/%J.out -W 24:00 ./batch_up_db_check.py $batch
done

#!/bin/bash
# Run from /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp
odir=lotus-logs/db-builder/
mkdir -p $odir

for batch in $(seq 0 248); do
    bsub -o ./$odir/%J.out -W 24:00 ./setup_environment_lotus.sh $batch
done

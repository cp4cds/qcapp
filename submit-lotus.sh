#!/bin/bash

# Ensure a output directory for lotus logs exists
odir=/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/lotus-logs/db-utd
mkdir -p $odir

var=$1
table=$2
freq=$3

# Submit to lotus "run_qc_lotus.sh" with arguments: variable, table, frequency set up a job on louts
bsub -o $odir/%J.out -W 24:00 ./run_qc_lotus.sh $var $table $freq


#!/bin/bash

# Ensure a output directory for lotus logs exists
var=$1
table=$2
freq=$3
logdir=$4
cmd_line_args=$5

odir=/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/lotus-logs/${logdir}
mkdir -p $odir

# Submit to lotus "run_qc_lotus.sh" with arguments: variable, table, frequency set up a job on louts
bsub -o $odir/%J.out -W 24:00 ./run_qc_lotus.sh $var $table $freq $cmd_line_args


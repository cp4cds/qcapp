#!/bin/bash

odir=/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/lotus-logs
mkdir -p $odir

var=$1
table=$2
freq=$3
expt=$4

bsub -o $odir/%J.out -W 24:00 ./run_qc_lotus.sh $var $table $freq $expt


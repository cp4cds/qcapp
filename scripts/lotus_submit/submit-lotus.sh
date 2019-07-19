#!/bin/bash

# Ensure a output directory for lotus logs exists
var=$1
logdir=$2
#args=$7

mkdir -p $logdir

# Submit to lotus "run_qc_lotus.sh" with arguments: variable, table, frequency set up a job on louts
#bsub -o $logdir/%J.out -W 24:00 ./setup_environment_lotus.sh $var $freq $table $expt $model $args
#bsub -o $logdir/%J.out -W 24:00 ./setup_environment_lotus.sh $var $freq $table

#bsub -o $logdir/%J.out -W 24:00 ./setup_environment_lotus.sh $var
bsub -o $logdir/%J.out -W 2:00 ./setup_environment_lotus.sh $var $freq $table $expt

#source ./run_qc_lotus.sh $var $table $freq $cmd_line_args

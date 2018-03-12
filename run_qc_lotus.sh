#!/bin/bash

var=$1
table=$2
freq=$3
cmd_line_args=$4

#echo "python /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/qc_db_builder.py " $var $table $freq $cmd_line_args > out.log
#python /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/qc_db_builder.py $var $table $freq $cmd_line_args

## For each job on the host ensure the correct virtual environment is activated
source /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/venv2/bin/activate

## For each job on the host ensure the Django settings are exported
export DJANGO_SETTINGS_MODULE=qcproj.settings

## Call the database builder "qc_db_builder.py" with the arguments variable, table, frequency and any command line arguments
python /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/qc_db_builder.py $var $table $freq $cmd_line_args

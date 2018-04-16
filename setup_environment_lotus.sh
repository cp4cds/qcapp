#!/bin/bash

batch=$1

## For each job on the host ensure the correct virtual environment is activated
source /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/venv2/bin/activate

## For each job on the host ensure the Django settings are exported
export DJANGO_SETTINGS_MODULE=qcproj.settings

## Call the python program with the arguments variable, table, frequency and any command line arguments
python /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/run_batch_db_builder.py ${batch}

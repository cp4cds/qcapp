#!/bin/bash

batch_no=$1

## For each job on the host ensure the correct virtual environment is activated
source /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/venv2/bin/activate
cd /group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/

## For each job on the host ensure the Django settings are exported
export DJANGO_SETTINGS_MODULE=qcproj.settings
export PYTHONPATH=$PWD:$PWD/scripts:$PWD/lotus_submit


## Call the python program with the arguments variable, table, frequency and any command line arguments
python /group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/scripts/copy_from_archive_batch_up.py $batch_no
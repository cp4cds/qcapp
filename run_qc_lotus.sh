#!/bin/bash

var=$1
table=$2
freq=$3

# For each job on the host ensure the correct virtual environment is activated
source /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/venv2/bin/activate
# For each job on the host ensure the Django settings are exported
export DJANGO_SETTINGS_MODULE=qcproj.settings

# Call the database builder "qc_db_builder.py" with the arguments variable, table, frequency

####
# Add which options are required here
####
python /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/qc_db_builder.py $var $table $freq --parse_cedacc

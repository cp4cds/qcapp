#!/bin/bash

var=$1
table=$2
freq=$3
expt=$4

/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/venv/bin/activate
export DJANGO_SETTINGS_MODULE=qcproj.settings
python /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/qc_db_builder.py $var $table $freq $expt

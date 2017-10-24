#!/bin/bash

/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/venv/bin/activate
export DJANGO_SETTINGS_MODULE=qcproj.settings
python /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/esgf_search_functions.py
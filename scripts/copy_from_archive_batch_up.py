#!/usr/bin/python2.7

import os
from sys import argv
from subprocess import call

# FAILED_DATAFILES = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/logfiles/fixable_datafiles.log'
# DATASETS_TO_FIX_DIR = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/status_logs/fix_2019-01-29/to_fix'

# DATASET_IDS_TO_FIX = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/status_logs/fix_2019-01-29/dataset_ids_to_fix_2019-02-01_r2.log'
start = int(argv[1])
PER_BATCH = 50
srt_idx = start * PER_BATCH
end_idx = srt_idx + PER_BATCH

filename = "/group_workspaces/jasmin2/cp4cds1/qc/meridional_wind/c3s-expts.txt"
with open(filename) as r:
    dataset_paths = [line.strip() for line in r]

for ds in dataset_paths[srt_idx: end_idx]:

    run_cmd = ["python", "/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/scripts/copy_from_archive_to_gws.py", ds.strip()]
    run = call(run_cmd)
    if run != 0:
        print("ERROR RUNNING %s" % run_cmd)
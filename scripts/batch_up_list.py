#!/usr/bin/python2.7

import os
from sys import argv
from subprocess import call

# FAILED_DATAFILES = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/logfiles/fixable_datafiles.log'
# DATASETS_TO_FIX_DIR = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/status_logs/fix_2019-01-29/to_fix'

DATASET_IDS_TO_FIX = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/status_logs/fix_2019-01-29/dataset_ids_to_fix_2019-02-01_r2.log'
start = int(argv[1])
PER_BATCH = 25
srt_idx = start * PER_BATCH
end_idx = srt_idx + PER_BATCH

# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/files_to_correct.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/new_v20180618_files_to_qc.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/corrected_files.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/publish_datasets_1.txt"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/failed_datasets.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/ingested_to_archive.log"

with open(DATASET_IDS_TO_FIX, 'r') as fr:
    datasets = fr.readlines()

for ds in datasets[srt_idx: end_idx]:

    run_cmd = ["python", "/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/scripts/fix_and_ingest_datasets.py", ds.strip()]
    run = call(run_cmd)
    if run != 0:
        print "ERROR RUNNING %s" % run_cmd
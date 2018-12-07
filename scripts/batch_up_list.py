#!/usr/bin/python2.7

from setup_django import *
import os
from sys import argv
from subprocess import call

FAILED_DATAFILES = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/logfiles/fixable_datafiles.log'

start = int(argv[1])
PER_BATCH = 200
srt_idx = start * PER_BATCH
end_idx = srt_idx + PER_BATCH

# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/files_to_correct.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/new_v20180618_files_to_qc.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/corrected_files.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/publish_datasets_1.txt"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/failed_datasets.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/ingested_to_archive.log"

with open(FAILED_DATAFILES, 'r') as fr:
    data = fr.readlines()

for df in data[srt_idx: end_idx]:
    run_cmd = ["python", "file_error_fixer.py", df.strip()]
    run = call(run_cmd)
    if run != 0:
        print "ERROR RUNNING %s" % run_cmd
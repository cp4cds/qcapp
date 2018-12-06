#!/usr/bin/python2.7

from setup_django import *
import os
from sys import argv
from subprocess import call

FAILED_DATASETS = '../ancil_files/failed_datasets.log'

start = int(argv[1])
DATASETS_PER_BATCH = 50
srt_idx = start * DATASETS_PER_BATCH
end_idx = srt_idx + DATASETS_PER_BATCH

# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/files_to_correct.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/new_v20180618_files_to_qc.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/corrected_files.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/publish_datasets_1.txt"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/failed_datasets.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/ingested_to_archive.log"

with open(FAILED_DATASETS, 'r') as fr:
    data = fr.readlines()

for dataset in data[srt_idx: end_idx]:
    run_cmd = ["python", "restructure.py", dataset.strip()]
    run = call(run_cmd)
    if run != 0:
        print "ERROR RUNNING %s" % run_cmd
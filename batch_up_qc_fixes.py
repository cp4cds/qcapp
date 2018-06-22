#!/usr/bin/python2.7
import os
from sys import argv
from subprocess import call

start = int(argv[1])
files_per_batch = 500
srt_idx = start * files_per_batch
end_idx = srt_idx + files_per_batch

# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/files_to_correct.log"
filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/new_v20180618_files_to_qc.log"

with open(filelist, 'r') as fr:
    files = fr.readlines()

for file in files[srt_idx: end_idx]:
    run_cmd = ["python", "qc_fixed_files_ingest_add_to_db.py", file.strip()]
    run = call(run_cmd)
    if run != 0:
        print "ERROR RUNNING %s" % run_cmd
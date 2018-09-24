#!/usr/bin/python2.7
import os
from sys import argv
from subprocess import call

start = int(argv[1])
files_per_batch = 300
srt_idx = start * files_per_batch
end_idx = srt_idx + files_per_batch

# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/files_to_correct.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/new_v20180618_files_to_qc.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/corrected_files.log"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/publish_datasets_1.txt"
# filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/failed_datasets.log"

filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/ingested_to_archive.log"

with open(filelist, 'r') as fr:
    files = fr.readlines()

for file in files[srt_idx: end_idx]:
    run_cmd = ["python", "qc_fixed_files_ingest_add_to_db.py", file.strip(), str(start)]
    run = call(run_cmd)
    if run != 0:
        print "ERROR RUNNING %s" % run_cmd
#!/usr/bin/python2.7
import os
from sys import argv
from subprocess import call

start = int(argv[1])
files_per_batch = 5000
srt_idx = start * files_per_batch
end_idx = srt_idx + files_per_batch

filelist = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/valid_all_cp4cds_filelist.log"
os.environ["DJANGO_SETTINGS_MODULE"] = "qcproj.settings"

with open(filelist, 'r') as fr:
    files = fr.readlines()

for file in files[srt_idx: end_idx]:
    run_cmd = ["/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/venv2/bin/python",
			   "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/test-in-database.py", file, str(start)]
    run = call(run_cmd)
    if run != 0:
        print "ERROR RUNNING %s" % run_cmd
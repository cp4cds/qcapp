
from setup_django import *
import os
import shutil
import sys
import re
import glob
from settings import *
import utils

dirs_to_fix = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/dirs_to_remove.log"

with open(dirs_to_fix) as r:
    paths = r.readlines()

for path in paths[1:]:
    path = path.strip()
    dir = '/'.join(path.split('/')[:-2])
    latest_dir = os.path.join(dir, 'latest')
    v2018_dir = os.path.join(dir, 'v20181001')
    os.unlink(latest_dir)
    shutil.rmtree(v2018_dir)
    dirs = os.listdir(dir)

    for d in dirs:
        if d.startswith('v'):
            vdir = d

    os.chdir(dir)
    os.symlink(vdir, 'latest')

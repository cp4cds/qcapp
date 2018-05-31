
import django
django.setup()

import os
import fnmatch
from subprocess import call
from qcapp.models import *

cclogdir = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/QC_LOGS'


def main():
    dfs = DataFile.objects.all()

    for df in dfs:

        ins, model, exp, freq, realm, table, ens, version, var, ncfile = df.archive_path.split('/')[6:]
        cclogfile_dir = os.path.join(cclogdir, var, table, exp, ens, version)
        cclogfile = ncfile.replace('.nc', '__qclog_')
        files = os.listdir(cclogfile_dir)
        found = False
        for f in files:
            if f.startswith(cclogfile):
                found = True
        if not found:
            print df.gws_path

if __name__ == "__main__":
    main()
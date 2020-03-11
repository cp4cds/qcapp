
from setup_django import *
import os
import sys
import shutil
import time

GWSDIR = "/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/output1/"
ARCHIVE = "/badc/cmip5/data/cmip5/output1/"

def main(ds_path):

    version = os.readlink('/'.join(ds_path.split("/")[:-1]))
    parts = ds_path.split('/')
    parts[-2] = version
    ds_version_src_path = '/'.join(parts)


    ds_version_dst_path = ds_version_src_path.replace(ARCHIVE, GWSDIR)
    dst_parts = ds_version_dst_path.split('/')
    dst_parts[-2] = dst_parts[-1]
    dst_parts[-1] = "files"
    dst_parts.append(version.strip('v'))
    ds_version_dst_path = '/'.join(dst_parts) + "/"
    print(ds_version_dst_path)


    # print(ds_version_src_path, ds_version_dst_path)
    for f in os.listdir(ds_version_src_path):
        fname = os.path.join(ds_version_src_path, f)
        # print(fname)
        # print(ds_version_dst_path)
        if not os.path.exists(ds_version_dst_path):
            os.makedirs(ds_version_dst_path)
        try:
            shutil.copy(fname, ds_version_dst_path)
        except(IOError):
            time.sleep(4)
            shutil.copy(fname, ds_version_dst_path)

if __name__ == "__main__":

    ds_path = sys.argv[1]
    main(ds_path)
"""
A script routine that will edit
"""
import django
django.setup()

import os
import json
import requests
import commands
import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from sys import argv
from qcapp.models import *
from utils import *

requests.packages.urllib3.disable_warnings()
ARCHIVE_ROOT = "/badc/cmip5/data/"
WEBROOT = "http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/"
ARCHIVE_BASEDIR = "/badc/cmip5/data/cmip5/output1/"
GWS_BASEDIR = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/"
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"

# for datafiles in DataFile.objects.all():

def add_gws_field():

    df = DataFile.objects.first()
    print(df.archive_path)
    path = df.archive_path.replace(ARCHIVE_BASEDIR, GWS_BASEDIR)
    print(path)
    path_list = path.split('/')

    if path_list[-3] == "files":
        path_list.pop(-3)

    # Change version to latest
    # version = path_list[-2]
    path_list[-2] = "latest"

    gws_latest_path = "/".join(path_list)
    print(gws_latest_path)
    print(os.path.exists(gws_latest_path))

    gws_path = 

    # path = ipath.replace(dir1, dir2)
    # path_list = path.split('/')
    # version = path_list[-2]
    # if not version.startswith('v'):
    #     version = "v"+version
    #     path_list[-2] = version
    # if path_list[-3] == "files":
    #     path_list.pop(-3)
    #     gws_path = "/".join(path_list)
    #     return gws_path
    #
    #
    # path_list[-3], path_list[-2] = path_list[-2], path_list[-3]
    # gws_path = "/".join(path_list)
    # return gws_path



if __name__ == "__main__":
    add_gws_field()
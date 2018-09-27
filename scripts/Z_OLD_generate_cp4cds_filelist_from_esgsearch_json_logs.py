#!/usr/bin/env python
import django
django.setup()

import os
from sys import argv
from qcapp.models import *
import json
import requests
import commands
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import datetime
requests.packages.urllib3.disable_warnings()
ARCHIVE_ROOT = "/badc/cmip5/data/"
FILELIST = "ancil_files/cp4cds_all_vars.txt"
ALLEXPTS = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"
WEBROOT = "http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/"
ARCHIVE_BASEDIR = "/badc/cmip5/data/cmip5/output1/"
GWS_BASEDIR = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/"


def _convert_path(ipath):
    path = ipath.replace(ARCHIVE_BASEDIR, GWS_BASEDIR)
    path_list = path.split('/')
    path_list[-3], path_list[-2] = path_list[-2], path_list[-3]
    gws_path = "/".join(path_list)

    return gws_path


def main():

    with open(FILELIST) as fr:
        data = fr.readlines()

    for line in data:
        line = line.strip()
        variable, frequency, table = line.split(',')
        for experiment in ALLEXPTS:
            json_filename = '.'.join([variable, frequency, table, experiment])
            json_file = os.path.join(JSONDIR, json_filename)
            with open(json_file) as fr:
                datafiles = json.load(open(json_file))

            for df in datafiles:
                # df = datafiles[0]
                archive_file = df['url'][0].split('|')[0].replace(WEBROOT, ARCHIVE_ROOT)
                print(archive_file)
                gws_file = _convert_path(archive_file)
                if os.path.exists(gws_file):
                    with open("valid_esg_gws_files.log", 'a+') as fw:
                        fw.writelines([gws_file, '\n'])
                else:
                    with open("invalid_esg_gws_files.log", 'a+') as fw:
                        fw.writelines([gws_file, '\n'])



if __name__ == "__main__":

    main()

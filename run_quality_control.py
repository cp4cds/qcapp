#!/usr/bin/env python
"""
A driver routine that will run the quality control related routines specified.
"""
import django
django.setup()

import os
import json
import requests
import commands
import datetime
import argparse
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from sys import argv
from qcapp.models import *
from utils import *
from qc_functions import *


requests.packages.urllib3.disable_warnings()
ARCHIVE_ROOT = "/badc/cmip5/data/"
WEBROOT = "http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/"
ARCHIVE_BASEDIR = "/badc/cmip5/data/cmip5/output1/"
GWS_BASEDIR = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/"
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"

parser = argparse.ArgumentParser()
parser.add_argument('--ceda_cc',action='store_true', help='Run CEDA-CC')
parser.add_argument("-i", dest="filename", required=True, help="input file", metavar="FILE")
                    # type=lambda x: is_valid_file(parser, x)

if __name__ == "__main__":

    # /badc/cmip5/data/cmip5/output1/MOHC/HadGEM2-ES/rcp45/mon/atmos/Amon/r1i1p1/tas/files/20111128/tas_Amon_HadGEM2-ES_rcp45_r1i1p1_212412-214911.nc
    args = parser.parse_args()

    gws_file = convert_to_cp4cds_gws_path(args.filename, ARCHIVE_BASEDIR, GWS_BASEDIR)
    print(gws_file)
    if args.ceda_cc:
        run_ceda_cc(gws_file, '.')
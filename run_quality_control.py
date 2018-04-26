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
from time_checks.run_file_timechecks import main as single_file_time_checks
from time_checks.run_multifile_timechecks import main as multi_file_time_checks
from qcapp.models import *
from utils import *
from qc_functions import *
from is_latest import *

requests.packages.urllib3.disable_warnings()
ARCHIVE_ROOT = "/badc/cmip5/data/"
WEBROOT = "http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/"
ARCHIVE_BASEDIR = "/badc/cmip5/data/cmip5/output1/"
GWS_BASEDIR = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/"
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"

parser = argparse.ArgumentParser()
parser.add_argument('variable', type=str, help='A CP4CDS variable')
parser.add_argument('frequency', type=str, help='A CP4CDS frequency')
parser.add_argument('table', type=str, help='A CP4CDS table')
parser.add_argument('--ceda_cc',action='store_true', help='Run CEDA-CC')
parser.add_argument('--parse_ceda_cc',action='store_true', help='Parse CEDA-CC output')
parser.add_argument('--cf_checker',action='store_true', help='Run CF-Checker')
parser.add_argument('--parse_cf_checker',action='store_true', help='Parse CF-Checker output')
parser.add_argument('--single_file_time_check', action='store_true', help="Run the single file time checks")
parser.add_argument('--multifile_time_check', action='store_true', help="Run the multifile time checks")
parser.add_argument('--is_latest', action='store_true', help="Work out if a datafile is the latest")
# parser.add_argument("-i", dest="filename", required=True, help="input file", metavar="FILE")
                    # type=lambda x: is_valid_file(parser, x)


def main(args):

    if args.ceda_cc or args.parse_ceda_cc or args.cf_checker or args.single_file_time_check:

        for df in DataFile.objects.filter(variable=args.variable, dataset__frequency=args.frequency,
                                          dataset__cmor_table=args.table, dataset__experiment=args.experiment):
            ensemble = df.gws_path.split('/')[-4]
            version = "v" + os.readlink(df.gws_path).split('/')[-2]
            odir = os.path.join(QCLOGS, args.variable, args.table, args.experiment, ensemble, version)

            if not os.path.isdir(odir):
                os.makedirs(odir)

            if args.ceda_cc:
                run_ceda_cc(df.gws_path, odir)

            if args.parse_ceda_cc:
                parse_ceda_cc(df, odir)

            if args.cf_checker:
                run_cf_checker(df.gws_path, odir)

            if args.single_file_time_check:
                file_time_checks(df.gws_path, odir)


    if args.multifile_time_check:

        dss = Dataset.objects.filter(variable=args.variable, cmor_table=args.table, frequency=args.frequency,
                                     experiment=args.experiment)
        run_multifile_time_checker(dss, args.variable, args.table, args.experiment)


    if args.is_latest:
        run_is_latest(args.variable, args.frequency, args.table)

if __name__ == "__main__":

    # /badc/cmip5/data/cmip5/output1/MOHC/HadGEM2-ES/rcp45/mon/atmos/Amon/r1i1p1/tas/files/20111128/tas_Amon_HadGEM2-ES_rcp45_r1i1p1_212412-214911.nc
    args = parser.parse_args()
    main(args)


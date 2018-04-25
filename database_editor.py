"""
A script routine that will edit
"""
import django
django.setup()

import os
import fnmatch
import json
import requests
import commands
import datetime
import argparse
from subprocess import call
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from sys import argv
from qcapp.models import *
from utils import *
from qc_settings import *

requests.packages.urllib3.disable_warnings()
ARCHIVE_ROOT = "/badc/cmip5/data/"
WEBROOT = "http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/"
ARCHIVE_BASEDIR = "/badc/cmip5/data/cmip5/output1/"
GWS_BASEDIR = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/"
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"

# for datafiles in DataFile.objects.all():

parser = argparse.ArgumentParser()
parser.add_argument('--add_gws_path',action='store_true', help='Add the group workspace path to the database datafile table')
parser.add_argument('--set_restricted_status',action='store_true', help='Set the open or restricted status of the data')
parser.add_argument('--remove_restricted_datasets',action='store_true', help='Delete restricted dataset records')
parser.add_argument('--find_missing_ceda_cc', action='store_true', help='Generate a list of any missing CEDA-CC files')
parser.add_argument('--check_number_qc_files', action='store_true', help='Calculate the number of qc files and compare with expected')
parser.add_argument('--move_cferr_files', action='store_true', help='Move the cf-err.log files from the /data dir to /CF-FATAL dir')

def move_cf_error_files():

    with open("cf-err_files.log") as fr:
        files = fr.readlines()

    for file in files:
        file = file.strip()
        if os.path.getsize(file) == 0:
            if os.path.basename(file).endswith('.cf-err'):
                print "REMOVING {}".format(file)
                # os.remove(file)
        else:
            institute, model, experiment, frequency, realm, table, ensemble, variable, version, errfile = file.split('/')[8:]
            logdir = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/CF-FATAL-LOGS"
            logfile = os.path.join(logdir, variable, table, experiment, ensemble, version, errfile)
            os.makedirs(os.path.dirname(logfile))
            touch_cmd = ["touch", logfile]
            call(touch_cmd)


def check_number_of_files():
    vars_file = "ancil_files/cp4cds_all_vars.txt"
    delimiter = ','
    with open(vars_file) as reader:
        data = reader.readlines()
        for line in data:
            variable = line.split(delimiter)[0].strip()
            frequency = line.split(delimiter)[1].strip()

            for experiment in ALLEXPTS:

                ndfs = DataFile.objects.filter(variable=variable,
                                               dataset__frequency=frequency,
                                               dataset__experiment=experiment).count()
                dir_path = os.path.join('../CEDACC_LOGS', variable, frequency, experiment)
                try:
                    nfiles = len(fnmatch.filter(os.listdir(dir_path), '*__qclog_*.txt'))
                except:
                    pass
                if not nfiles == ndfs:
                #     print("MATCH {} files for {}, {}, {}".format(nfiles, variable, frequency, experiment))
                # else:
                    print("ERROR - NO MATCH {} files : {} records for {}, {}, {}".format(nfiles, ndfs, variable, frequency, experiment))

def add_gws_field():

    datafiles = DataFile.objects.all()
    for df in datafiles:
        print(df.archive_path)
        path = df.archive_path.replace(ARCHIVE_BASEDIR, GWS_BASEDIR)
        path_list = path.split('/')

        if path_list[-3] == "files":
            path_list.pop(-3)

        # Change version to latest
        # version = path_list[-2]
        path_list[-2] = "latest"

        gws_latest_path = "/".join(path_list)

        if os.path.exists(gws_latest_path):
            df.gws_path = gws_latest_path
            df.save()

        else:
            with open("gws_files_err.log", "a+") as fw:
                fw.writelines([gws_latest_path + "\n"])


def set_restricted_flag():

    for df in DataFile.objects.all():

        restricted_models = ['MIROC4h', 'MIROC5', 'MIROC-ESM', 'MIROC-ESM-CHEM', 'MRI-AGCM3-2H', 'MRI-AGCM3-2S',
                             'MRI-CGCM3', 'MRI-ESM1', 'NICAM-09']
        df_model = df.dataset.model
        if df_model in restricted_models:
            df.restricted = True
        else:
            df.restricted = False

        print df.restricted
        df.save()


def delete_restricted_datasets():

    restricted_models = ['MIROC4h', 'MIROC5', 'MIROC-ESM', 'MIROC-ESM-CHEM', 'MRI-AGCM3-2H', 'MRI-AGCM3-2S',
                         'MRI-CGCM3', 'MRI-ESM1', 'NICAM-09']

    for ds in Dataset.objects.all():

        if ds.model in restricted_models:
            print("Removed:{}".format(ds.dataset_id))
            ds.delete()

def get_missing_ceda_cc_filelist():

    for df in DataFile.objects.all():

        institute, model, experiment, frequency, realm, table, ensemble, variable, latest, ncfile = df.gws_path.split('/')[8:]
        temporal_range = ncfile.strip(".nc").split("_")[-1]
        file_base = "_".join([variable, table, model, experiment, ensemble, temporal_range])
        logdir = os.path.join(CEDACC_DIR, variable, frequency, experiment)
        log_dir_files = os.listdir(logdir)

        # Constructs a CEDA-CC regex based on variable_table_model_experiment_ensemble_temporal-range__qclog_{date}.txt
        ceda_cc_file_pattern = re.compile(file_base + "__qclog_\d+\.txt")

        ceda_cc_file = next((f for f in log_dir_files if ceda_cc_file_pattern.match(f)), None)
        if ceda_cc_file:
            print "{}:EXISTS \n".format(ceda_cc_file)
        else:
            print "{}:DOES NOT EXIST \n".format(df.gws_path)

if __name__ == "__main__":

    args = parser.parse_args()
    if args.add_gws_path:
        add_gws_field()

    if args.set_restricted_status:
        set_restricted_flag()

    if args.remove_restricted_datasets:
        delete_restricted_datasets()

    if args.find_missing_ceda_cc:
        get_missing_ceda_cc_filelist()

    if args.check_number_qc_files:
        check_number_of_files()

    if args.move_cferr_files:
        move_cf_error_files()
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
import filecmp

from subprocess import call
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from sys import argv
from qcapp.models import *
from utils import *
from qc_settings import *
from qc_functions import *

requests.packages.urllib3.disable_warnings()
ARCHIVE_ROOT = "/badc/cmip5/data/"
WEBROOT = "http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/"
ARCHIVE_BASEDIR = "/badc/cmip5/data/cmip5/output1/"
GWS_BASEDIR = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/"
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"

# for datafiles in DataFile.objects.all():

parser = argparse.ArgumentParser()
parser.add_argument('variable', type=str, nargs='?', help='A CP4CDS variable')
parser.add_argument('frequency', type=str, nargs='?', help='A CP4CDS frequency')
parser.add_argument('table', type=str, nargs='?', help='A CP4CDS table')
parser.add_argument('experiment', type=str, nargs='?', help='A CP4CDS experiment')

parser.add_argument('--add_gws_path',action='store_true', help='Add the group workspace path to the database datafile table')
parser.add_argument('--set_restricted_status',action='store_true', help='Set the open or restricted status of the data')
parser.add_argument('--remove_restricted_datasets',action='store_true', help='Delete restricted dataset records')
parser.add_argument('--find_missing_ceda_cc', action='store_true', help='Generate a list of any missing CEDA-CC files')
parser.add_argument('--check_number_qc_files', action='store_true', help='Calculate the number of qc files and compare with expected')
parser.add_argument('--move_cferr_files', action='store_true', help='Move the cf-err.log files from the /data dir to /CF-FATAL dir')
parser.add_argument('--fix_archivepath_checksums', action='store_true', help='Correct the archive path and recalculate all the md5 checksums for all datafiles')
parser.add_argument('--fix_cfchecker_output', action='store_true', help='Check all cf-checker err logs are needed by re-running the checker')
parser.add_argument('--create_islatest_qclog', action='store_true', help='Move the information in the Datafile up_to_date_note to a QC record')
parser.add_argument('--create_duplicates', action='store_true', help='Move the information in the Datafile up_to_date_note to a QC record')
parser.add_argument('--create_update_list', action='store_true', help='Move the information in the Datafile up_to_date_note to a QC record')
parser.add_argument('--fix_latest_dataset', action='store_true', help='Fix the latest Dataset version')
parser.add_argument('--fix_new_version_dirs', action='store_true', help='Fix the new version dataset directories')


def fix_new_version_dirs():

    dfs = DataFile.objects.filter(new_dataset_version=True)

    for df in dfs[1:]:
        print df.gws_path
        filebasedir = os.path.dirname(df.gws_path).strip('latest')
        os.chdir(filebasedir)
        new_version_no = os.readlink('latest').strip('v')
        os.chdir('files')
        dirs = os.listdir('.')


        if len(dirs) > 1:
            print "MULTIPLE VERSIONS EXIST".format(df.gws_path)
            continue
        else:
            files_to_link = os.listdir(dirs[0])

        if not os.path.isdir(new_version_no):
            os.makedirs(new_version_no)
        else:
            print "Directory already exists"
            continue

        # FILES DIR
        for f in files_to_link:
            target = os.path.join('..', dirs[0], f)
            linkname = os.path.join(new_version_no, f)
            os.symlink(target, linkname)


        # VERSION DIR
        os.chdir(os.path.join('..', 'v'+new_version_no))
        for f in files_to_link:
            target = os.path.join('..', 'files', new_version_no, f)
            linkname = f
            if os.path.exists(linkname):
                os.remove(linkname)

            os.symlink(target, linkname)

def fix_latest_dataset():
    # done = [
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CMS/rcp85/mon/atmos/Amon/r1i1p1/ps/latest/ps_Amon_CMCC-CMS_rcp85_r1i1p1_208001-208912.nc',
    #     # '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/ICHEC/EC-EARTH/rcp45/mon/atmos/Amon/r13i1p1/ps/latest/ps_Amon_EC-EARTH_rcp45_r13i1p1_201001-201012.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CESM/historical/mon/atmos/Amon/r1i1p1/ps/latest/ps_Amon_CMCC-CESM_historical_r1i1p1_187001-187412.nc',
    #     # '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/ICHEC/EC-EARTH/rcp45/mon/atmos/Amon/r13i1p1/ps/latest/ps_Amon_EC-EARTH_rcp45_r13i1p1_202801-202812.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp45/mon/atmos/Amon/r3i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp45_r3i1p3_210101-212512.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp85/mon/atmos/Amon/r2i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp85_r2i1p3_205101-207512.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CMS/rcp85/mon/atmos/Amon/r1i1p1/ts/latest/ts_Amon_CMCC-CMS_rcp85_r1i1p1_207001-207912.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-H/rcp85/mon/atmos/Amon/r1i1p3/ts/latest/ts_Amon_GISS-E2-H_rcp85_r1i1p3_225101-230012.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CESM/piControl/mon/atmos/Amon/r1i1p1/ts/latest/ts_Amon_CMCC-CESM_piControl_r1i1p1_439001-439512.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp26/mon/atmos/Amon/r1i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp26_r1i1p3_220101-222512.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp26/mon/atmos/Amon/r1i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp26_r1i1p3_207601-210012.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CESM/historical/mon/atmos/Amon/r1i1p1/ps/latest/ps_Amon_CMCC-CESM_historical_r1i1p1_187001-187412.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp45/mon/atmos/Amon/r3i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp45_r3i1p3_210101-212512.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CMS/rcp85/mon/atmos/Amon/r1i1p1/ts/latest/ts_Amon_CMCC-CMS_rcp85_r1i1p1_207001-207912.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-H/rcp85/mon/atmos/Amon/r1i1p3/ts/latest/ts_Amon_GISS-E2-H_rcp85_r1i1p3_225101-230012.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CMS/rcp85/mon/atmos/Amon/r1i1p1/ps/latest/ps_Amon_CMCC-CMS_rcp85_r1i1p1_208001-208912.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/piControl/mon/atmos/Amon/r1i1p1/tas/latest/tas_Amon_GISS-E2-R_piControl_r1i1p1_435601-438012.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp45/mon/atmos/Amon/r3i1p1/tas/latest/tas_Amon_GISS-E2-R_rcp45_r3i1p1_215101-217512.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp45/mon/atmos/Amon/r3i1p1/tas/latest/tas_Amon_GISS-E2-R_rcp45_r3i1p1_217601-220012.nc',
    #     '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp45/mon/atmos/Amon/r3i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp45_r3i1p3_210101-212512.nc',
    #     ]

    dfs = DataFile.objects.filter(new_dataset_version=True)

    with open('dataset_update_errors.log') as reader:
        files = reader.readlines()

    for f in files:
        f = f.strip()
        df = DataFile.objects.filter(gws_path=f).first()

    # for df in dfs[1:]:

        if df.dataset.supersedes == None:
            fname = df.gws_path
            print fname
            variable = fname.split('/')[-1].split('_')[0]
            version = "v{}".format(os.readlink(df.gws_path).split('/')[-2])
            version = format(os.readlink(os.path.dirname(df.gws_path)).split('/')[-1])
            old_version = df.archive_path.split('/')[-3]
            drs = df.dataset.esgf_drs
            drs_parts = drs.split('.')
            drs_parts[0] = drs_parts[0].upper()
            drs_parts.append(variable)
            drs_parts.append(version)
            dsid = '.'.join(drs_parts)
            # print dsid
            odrs = df.dataset.esgf_drs
            odrs_parts = odrs.split('.')
            odrs_parts[0] = odrs_parts[0].upper()
            odrs_parts.append(variable)
            odrs_parts.append(old_version)
            odsid = '.'.join(odrs_parts)
            # print odsid
            ds = Dataset.objects.filter(dataset_id=dsid).first()
            old_ds = Dataset.objects.filter(dataset_id=odsid).first()


            if df.dataset.version == version:
                # print "versions ok"
                # print df.dataset.version
                # print df.dataset.supersedes
                if df.dataset.supersedes == None:
                    print "making supersede"
                    df.dataset.supersedes = old_ds
                    df.dataset.save()

            # elif not ds == None:
            #     # print "EXISTS"
            #     df.dataset = ds
            #     df.save()
            #
            # else:
            #     # print "NEW"
            #     orig_ds = df.dataset
            #     # print orig_ds
            #     new_ds = Dataset.objects.get(pk=df.dataset.pk)
            #     new_ds.pk = None
            #     new_ds.id = None
            #     new_ds.version = version
            #     new_ds.supersedes = orig_ds
            #     new_ds.save()
            #     # print new_ds
            #
            #     df.dataset = new_ds
            #     df.save()



def create_update_filelist():

    odir = '/group_workspaces/jasmin2/cp4cds1/synda/sdt/selection/cp4cds/cp4cds-update'
    dfs = DataFile.objects.filter(duplicate_of=None, up_to_date_note__contains="CHECKSUM")
    for d in dfs:
        drs = d.dataset.esgf_drs
        version = d.up_to_date_note.split(' :: ')[2].split(',')[1].split(' ')[-1]
        file = d.ncfile
        ofile = "{}.v{}.{}.txt".format(drs, version, file)
        id = "instance_id={}.v{}.{}\n".format(drs, version, file)
        ofile = os.path.join(odir, ofile)
        with open(ofile, 'a+') as fw:
            fw.writelines("protocol=gridftp\n")
            fw.writelines(id)


def make_duplicates():
    logfile = "../duplicate_files.txt"
    ensp2 = 'r1i1p2'
    with open(logfile) as fr:
        files = fr.readlines()

    for f in files:
        f = f.strip()
        fncfile = os.path.basename(f)
        df = DataFile.objects.filter(ncfile=fncfile, dataset__ensemble=ensp2)

        if df.count() == 1:
            orig = df.first()
        else:
            print "ERROR with file: {}".format(f)
            pass
        if not os.path.exists(orig.gws_path):
            print "ERROR gws_path doesn't exist: {}".format(f)
            pass

        var, table, model, expt, ensp1, temp = orig.ncfile.split('_')
        # ## CHECK CORRECT RECORD & FILE exist
        correct_ncfile = '_'.join([var, table, model, expt, ensp2, temp])
        correct_filepath = os.path.join(os.path.dirname(f), correct_ncfile)

        if not os.path.exists(correct_filepath):
            print "ERROR correct_path doesn't exist: {}".format(correct_filepath)
            pass

        correct_dfs = DataFile.objects.filter(ncfile=correct_ncfile, dataset__ensemble=ensp2)
        if correct_dfs.count() != 1:
            print "Multiple Records: {}".format(correct_filepath)
            pass
        df_correct = correct_dfs.first()
        orig.duplicate_of = df_correct
        orig.save()
        print correct_filepath
        


def make_qc_err_record(dfile, checkType, errorType, errorMessage, filepath):
    qc_err, _ = QCerror.objects.get_or_create(
        file=dfile, check_type=checkType, error_type=errorType,
        error_msg=errorMessage, report_filepath=filepath)


def make_is_latest_qcerror(datafiles):

    qc_type = "LATEST"
    for df in datafiles:
        if not df.up_to_date:
            make_qc_err_record(df, qc_type, df.up_to_date_note.split(' :: ')[0].strip('IS_LATEST [').strip(']'),
            df.up_to_date_note, df.up_to_date_note.split(' :: ')[-1])



def rerun_cfchecker():

    file = "cf-err-log-list.log"
    with open(file) as fr:
        files = fr.readlines()

    for file in files[1:]:
        cflogfile = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/" + file.strip().replace('cf-err.txt', 'cf-log.txt')
        with open(cflogfile) as fr:
            data = fr.readlines()
        ncfile = data[1].split(' ')[-1].strip()
        variable = ncfile.split('/')[-3]
        table = ncfile.split('/')[-5]
        experiment = ncfile.split('/')[-8]
        ensemble = ncfile.split('/')[-4]
        version = "v" + os.readlink(ncfile).split('/')[-2]
        odir = os.path.join(QCLOGS, variable, table, experiment, ensemble, version)
        run_cf_checker(ncfile, odir)

def fix_path_checksums(datafiles):

    for df in datafiles:

        if not os.path.exists(df.archive_path):
            n, b, c, d, proj, outp, inst, model, ex, fr, realm, table, ens, var, files, version, ncfile = df.archive_path.split('/')
            df.archive_path = os.path.join('/', b, c, d, proj, outp, inst, model, ex, fr, realm, table, ens, 'v' + version, var, ncfile)

        if len(df.md5_checksum) != 32:
            df.md5_checksum = commands.getoutput('md5sum ' + df.archive_path).split(' ')[0]

        df.save()

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

    if args.fix_new_version_dirs:
        fix_new_version_dirs()

    if args.fix_latest_dataset:
        fix_latest_dataset()

    if args.create_update_list:
        create_update_filelist()

    if args.create_duplicates:
        make_duplicates()

    if args.create_islatest_qclog:
        dfs = DataFile.objects.filter(variable=args.variable, dataset__frequency=args.frequency,
                                      dataset__cmor_table=args.table)
        make_is_latest_qcerror(dfs)

    if args.fix_cfchecker_output:
        rerun_cfchecker()

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

    if args.fix_archivepath_checksums:
        dfs = DataFile.objects.filter(variable=args.variable, dataset__frequency=args.frequency,
                                      dataset__cmor_table=args.table, dataset__experiment=args.experiment)
        fix_path_checksums(dfs)
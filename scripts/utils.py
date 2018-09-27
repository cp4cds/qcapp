
import os
import datetime
import re
import glob
import requests
import itertools
import json
from settings import *




def check_log_exists(file, qcdir, ext):

    qcfiles = os.listdir(qcdir)
    cc_logfile = "{}{}".format(os.path.basename(file).strip('.nc'), ext)
    for f in qcfiles:
        if f.startswith(cc_logfile):
            return True, f
    return False, ""



def get_and_make_logdir(datafile):
    inst, model, expt, freq, realm, table, ensemble, var, ver, ncfile = datafile.gws_path.split('/')[8:]
    if ver == 'latest':
        v_version = os.readlink(os.path.dirname(datafile.gws_path))
    else:
        v_version = ver
    logdir = os.path.join(QCLOGS, var, table, expt, ensemble, v_version)

    if not os.path.isdir(logdir):
        os.makedirs(logdir)

    return logdir

def read_json(file):
    """

    From a given url this routine returns the elements from ["response"]["docs"]

    :param url:
    :return:
    """
    with open(file) as r:
        jsn = r.read()

    data = json.loads(jsn)
    res = data["response"]["docs"]
    return res
def define_local_json_cache_names(variable, frequency, table, experiment):

    json_logdir = os.path.join(LOCAL_JSON_DF_CACHE)

    if not os.path.isdir(json_logdir):
        os.makedirs(json_logdir)

    logfile = "{}_{}_{}_{}.json".format(variable, frequency, table, experiment)
    json_file = os.path.join(json_logdir, logfile)

    return json_logdir, json_file

def convert_to_cp4cds_gws_path(ipath, dir1, dir2):
    path = ipath.replace(dir1, dir2)
    path_list = path.split('/')
    version = path_list[-2]
    if not version.startswith('v'):
        version = "v"+version
        path_list[-2] = version
    if path_list[-3] == "files":
        path_list.pop(-3)
        gws_path = "/".join(path_list)
        return gws_path


    path_list[-3], path_list[-2] = path_list[-2], path_list[-3]
    gws_path = "/".join(path_list)
    return gws_path


def get_start_end_times(frequency, fname):
    """
    Get start and end times from the filename
    :return:
    """

    if fname.endswith('.nc'):

        ncfile = os.path.basename(fname)
        timestamp = ncfile.strip('.nc').split('_')[-1]

        # IF timestamp is of the form YYYYMMDDHHMM-YYYYMMDDHHMM
        if len(timestamp) == 25:
            start_time = datetime.date(int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[7:8]))
            end_time = datetime.date(int(timestamp[-12:-8]), int(timestamp[-8:-6]), int(timestamp[-6:-4]))

        # IF timestamp is of the form YYYYMMDDHH-YYYYMMDDHH
        if len(timestamp) == 21:
            start_time = datetime.date(int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[7:8]))
            end_time = datetime.date(int(timestamp[-10:-6]), int(timestamp[-6:-4]), int(timestamp[-4:-2]))

        if frequency == 'mon':
            start_time = datetime.date(int(fname[-16:-12]), int(fname[-12:-10]), 01)
            end_mon = fname[-5:-3]
            if end_mon == '02':
                end_day = 28
            elif end_mon in ['04', '06', '09', '11']:
                end_day = 30
            else:
                end_day = 31
            end_time = datetime.date(int(fname[-9:-5]), int(fname[-5:-3]), end_day)

        if frequency == 'day':
            start_time = datetime.date(int(fname[-20:-16]), int(fname[-16:-14]), int(fname[-14:-12]))
            end_time = datetime.date(int(fname[-11:-7]), int(fname[-7:-5]), int(fname[-5:-3]))
    else:
        start_time = datetime.date(1900, 1, 1)
        end_time = datetime.date(1999, 12, 31)

    return start_time, end_time


def _generate_datafile_url(fname):

    template = "https://esgf-index1.ceda.ac.uk/esg-search/search/?type=File&" \
               "latest=true&distrib=true&project=CMIP5&title={}&format=application%2Fsolr%2Bjson".format(fname)

    return template

def is_timeseries(filepath):
    """

    Checks whether the file is part of a timeseries by checking whether it
    exists as a single file in its directory.

    Returns True if only file in the directory
    Returns False if there is more than one file in the directory.

    :param filepath: valid filepath
    TODO: Add in valid filepath check
    :return: Boolean
    """

    if os.path.isdir(os.path.dirname(filepath)):

        if len(os.listdir(os.path.dirname(filepath))) > 1:
           ts = True
        else:
           ts = False
    else:
        ts = None

    return ts


def get_start_end_times(frequency, fname):
    """

    From a file name e.g. tas_Amon_EC-EARTH_historical_r13i1p1_200001-200911.nc
    The final element here is the file temporal range.

    Currently only working with monthly and daily data and so only returning a date object

    TODO Improve this to cope with 3 and 6 hourly data.

    If the temporal element is monthly then it has only YYYY and MM but no DD component as required for a
    datetime.date object. Irrespective of calendar used in the data a standard calendar is assumed and a dummy DD is
    generated in order that a datetime.date object can be generated.

    TODO: Incorporate the calendar information(?)

    :param frequency: CMIP5 frequency
    :param fname: filename
    :return: tuple of datetime.date objects representing the start and end times
    """


    if fname.endswith('.nc'):

        ncfile = os.path.basename(fname)
        timestamp = ncfile.strip('.nc').split('_')[-1]

        # IF timestamp is of the form YYYYMMDDHHMM-YYYYMMDDHHMM
        if len(timestamp) == 25:
            start_time = datetime.date(int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[7:8]))
            end_time = datetime.date(int(timestamp[-12:-8]), int(timestamp[-8:-6]), int(timestamp[-6:-4]))

        # IF timestamp is of the form YYYYMMDDHH-YYYYMMDDHH
        if len(timestamp) == 21:
            start_time = datetime.date(int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[7:8]))
            end_time = datetime.date(int(timestamp[-10:-6]), int(timestamp[-6:-4]), int(timestamp[-4:-2]))

        if frequency == 'mon':
            start_time = datetime.date(int(fname[-16:-12]), int(fname[-12:-10]), 01)
            end_mon = fname[-5:-3]
            if end_mon == '02':
                end_day = 28
            elif end_mon in ['04', '06', '09', '11']:
                end_day = 30
            else:
                end_day = 31
            end_time = datetime.date(int(fname[-9:-5]), int(fname[-5:-3]), end_day)

        if frequency == 'day':
            start_time = datetime.date(int(fname[-20:-16]), int(fname[-16:-14]), int(fname[-14:-12]))
            end_time = datetime.date(int(fname[-11:-7]), int(fname[-7:-5]), int(fname[-5:-3]))
    else:
        start_time = datetime.date(1900, 1, 1)
        end_time = datetime.date(1999, 12, 31)

    return start_time, end_time


def generate_filelist(FILELIST):
    """

    Generate a full list of all files in the QC db
    This is a debugging function and does not run in parallel context

    :param FILELIST: A global variable
    """
    # Ensure output file exists
    call(['touch', FILELIST])

    with open(FILELIST, 'a') as fw:
        for df in DataFiles.objects.all():
            fw.writelines([df.archive_path, "\n"])



def convert_archivepath_to_gwspath(arch_path):

    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = arch_path.split('/')[6:]
    alpha_base = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1"
    gws_path = os.path.join(alpha_base, institute, model, experiment, frequency, realm, table, ensemble, variable,
                            'latest', ncfile)
    return gws_path


def clear_cedacc_ouptut():
    """
    Tidy up any ceda-cc output files
    Move ceda-cc output files to a log_dir

    :return:
    """

    # Ensure log_dir exists
    logdir = os.path.join(QCAPP_PATH, 'log_dir')
    if not os.path.isdir(logdir):
        os.makedirs(logdir)

    # List of ceda-cc output files
    cedacc_ofiles = ["cccc_atMapLog.txt",
                     "amapDraft.txt"
                     "Rec.json",
                     "Rec.txt"]

    # If CEDA-CC output exists put this into a log_dir
    for f in cedacc_ofiles:
        filepath = os.path.join(QCAPP_PATH, f)
        if os.path.isfile(filepath):
            mv_cmd = ['mv', filepath, logdir]
            res = call(mv_cmd)

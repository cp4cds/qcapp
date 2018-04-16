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
WEBROOT = "http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/"
ARCHIVE_BASEDIR = "/badc/cmip5/data/cmip5/output1/"
GWS_BASEDIR = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/"
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"

def _convert_path(ipath):
    path = ipath.replace(ARCHIVE_BASEDIR, GWS_BASEDIR)
    path_list = path.split('/')
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


def _format_gen_url(template, **kwargs):
    return template.format(**kwargs)


def read_file_list(logfile):

    with open(logfile) as fr:
        data = fr.readlines()

    return data


def read_json(url):

    resp = requests.get(url, verify=False)
    json_resp = resp.json()
    return json_resp["response"]["docs"]

def generate_search_url(ifile):

    url_template = "https://{node}/esg-search/search?type=File&" \
                   "project=CMIP5&title={ncfile}" \
                   "&distrib={distrib}&latest={latest}" \
                   "&format=application%2Fsolr%2Bjson&limit=10000"

    return _format_gen_url(url_template, node="esgf-index1.ceda.ac.uk", ncfile=ifile, distrib="false", latest="true")

def check_not_in_database(ifile):

    df = DataFile.objects.filter(ncfile=ifile)
    if len(df) == 1:
        with open("database-fixer_errors.log", "a+") as fw:
            fw.writelines("{} Big problem datafile in database".format(ifile))
        return False
    else:
        return True


def create_database_entry(ipath, df_data):

    dRequester, _ = DataRequester.objects.get_or_create(requested_by="CP4CDS")
    # /group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/
    # output1/MOHC/HadGEM2-ES/rcp45/mon/atmos/Amon/r1i1p1/tas/files/20111128/
    # tas_Amon_HadGEM2-ES_rcp45_r1i1p1_212412-214911.nc
    product, institution, model, experiment, frequency, realm, table, ensemble, variable, \
        files, version, ncfile = ipath.split('/')[7:]
    version = "v{}".format(version)

    dSpec, _ = DataSpecification.objects.get_or_create(variable=variable, cmor_table=table, frequency=frequency)
    dSpec.datarequesters.add(dRequester)
    dSpec.save()

    drs = '.'.join(['cmip5', product, institution, model, experiment, frequency, realm, table, ensemble])
    dSet, _ = Dataset.objects.get_or_create(project="CMIP5",
                                            product=product,
                                            institute=institution,
                                            model=model,
                                            experiment=experiment,
                                            frequency=frequency,
                                            realm=realm,
                                            cmor_table=table,
                                            ensemble=ensemble,
                                            variable=variable,
                                            version=version,
                                            esgf_drs=drs,
                                            esgf_node='esgf-data1.ceda.ac.uk'
                                            )

    archive_filepath = ipath.replace(GWS_BASEDIR, ARCHIVE_BASEDIR)

    start_time, end_time = get_start_end_times(frequency, archive_filepath)
    md5_checksum = commands.getoutput('md5sum ' + archive_filepath).split(' ')[0]
    isTimeseries = is_timeseries(archive_filepath)

    if df_data:
        if df_data["checksum_type"][0].strip() == "SHA256":
            sha256_checksum = df_data["checksum"][0].strip()
        else:
            sha256_checksum = commands.getoutput('sha256sum ' + archive_filepath).split(' ')[0]
        fsize = df_data["size"]
        ftracking = df_data["tracking_id"][0].strip()
        furl = df_data["url"][0].strip()
        flongname = df_data["variable_long_name"][0].strip()
        fstandardname = df_data["cf_standard_name"][0].strip()
        funits = df_data["variable_units"][0].strip()
        published = True
    else:
        sha256_checksum = commands.getoutput('sha256sum ' + archive_filepath).split(' ')[0]
        fsize = 0
        ftracking = ""
        furl = ""
        flongname = ""
        fstandardname = ""
        funits = ""
        published = False


    dFile, _ = DataFile.objects.get_or_create(dataset=dSet,
                                              archive_path=archive_filepath,
                                              ncfile=os.path.basename(archive_filepath),
                                              size=fsize,
                                              sha256_checksum=sha256_checksum,
                                              md5_checksum=md5_checksum,
                                              tracking_id=ftracking,
                                              download_url=furl,
                                              variable=variable,
                                              variable_long_name=flongname,
                                              cf_standard_name=fstandardname,
                                              variable_units=funits,
                                              start_time=start_time,
                                              end_time=end_time,
                                              published=published,
                                              timeseries=isTimeseries
                                              )

def fix_database():

    files = read_file_list("missing_not_in_database_filelist_less_done.log")

    for file in files:
        file = file.strip()
        print (file)
        not_in_database = check_not_in_database(file)

        if not_in_database:
           url = generate_search_url(file)
           print(url)

           datafiles_info = read_json(url)
           if len(datafiles_info) == 1:
               create_database_entry(file, datafiles_info[0])
           elif len(datafiles_info) == 0:
               with open("missing_not_published.log", "a+") as fw:
                   fw.writelines("{} :: Not published at CEDA".format(file))
           else:
               for datafile in datafiles_info:
                   create_database_entry(file, datafile)
        else:
            with open("missing_in_database.log", "a+") as fw:
                fw.writelines("{} :: IN CP4CDS DB".format(file))


def build_database(file):

    # check file exists in gws
    if not os.path.exists(file):
        with open("database_builder_error.log", 'a+') as fw:
            fw.writelines([file, '\n'])
    else:
        print(file)
        project, output, institution, model, experiment, frequency, realm, table, \
        ensemble, variable, files, version, ncfile = file.split('/')[6:]
        instance = '.'.join(['cmip5', output, institution, model, experiment, frequency, realm, table, ensemble,
                             'v' + version, ncfile])
        json_filename = '.'.join([variable, frequency, table, experiment])
        json_file = os.path.join(JSONDIR, json_filename)
        datafiles = json.load(open(json_file))
        # Find the datafile entry that matches the instance id for the file
        df = next((d for d in datafiles if d['instance_id'] == instance), None)
        create_database_entry(file, df)

if __name__ == "__main__":

    ifile = argv[1]
    build_database(ifile)
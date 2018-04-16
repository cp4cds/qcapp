#!/usr/bin/env python
"""
A driver routine that will take as an option a single file and generate all QC-app tables from this.
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


def read_file_list(ifile):
    """

    Takes a text input file reads and returns contents

    :param ifile:
    :return:
    """
    with open(ifile) as fr:
        data = fr.readlines()

    return data


def read_json(url):
    """

    From a given url this routine returns the elements from ["response"]["docs"]

    :param url:
    :return:
    """

    resp = requests.get(url, verify=False)
    json_resp = resp.json()
    return json_resp["response"]["docs"]


def generate_search_url(ifile):

    """
    For a given file generates an ESGF search url

    :param ifile:
    :return:
    """

    url_template = "https://{node}/esg-search/search?type=File&" \
                   "project=CMIP5&title={ncfile}" \
                   "&distrib={distrib}&latest={latest}" \
                   "&format=application%2Fsolr%2Bjson&limit=10000"

    return _format_gen_url(url_template, node="esgf-index1.ceda.ac.uk", ncfile=ifile, distrib="false", latest="true")


def check_not_in_database(ifile):

    """
    For a given file, this checks that there is not already a database entry

    :param ifile:
    :return:
    """
    df = DataFile.objects.filter(ncfile=ifile)
    if len(df) == 1:
        with open("database-fixer_errors.log", "a+") as fw:
            fw.writelines("{} Big problem datafile in database".format(ifile))
        return False
    else:
        return True


def create_database_entry(ipath, df_data):

    """

    Given a datafile path and its ESGF datafile information create all
    necessary database information

    :param ipath: filepath
    :param df_data: JSON information generated by ESGF search
    :return:
    """

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

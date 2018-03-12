
"""
Usage:
  qc_db_builder.py  [VAR] [TABLE] [FREQ]
                    [--create] [--run_cedacc] [--parse_cedacc] [--run_cfchecker] [--parse_cfchecker]
                    [--check_up_to_date] [--run_single_file_timechecks] [--run_multi_file_timechecks]
                    [--test] [--esgf-ds-logger] [--check_data_is_latest] [--generate_latest_cache ]
                    [--dataset] [--datafile] [--is_latest_consistent]

Arguments:
    VAR         A valid CMIP5 short variable name
    TABLE       A valid CMIP5 table name
    FREQ        A valid CMIP5 frequency

Options:
    --create                            Create "dataset" and "datafile" records for a given set of
                                        input parameters: variable, frequency and table.
    --test                              Prints out the input arguments.
    --esgf-ds-logger                    Do a CMIP5 log of datafile info.
    --run_cedacc                        Run CEDA-CC for all files in all experiments with the given input parameters.
    --parse_cedacc                      Parse the CEDA-CC output for all files in all experiments with the given input parameters.
    --run_cfchecker                     Run the CF-Checker for all files in all experiments with the given input parameters.
    --parse_cfchecker                   Parse the CF-Checker output for all files in all experiments with the given input parameters.
    --check_up_to_date                  Checks if the most recent version is at CEDA
    --run_single_file_timechecks        Run single file time-checks for all files in all experiments with the given input parameters.
    --run_multi_file_timechecks         Run multifile timeseries completeness checks
    --check_data_is_latest              Run code to check whether local data is up to date
    --generate_latest_cache             Generate JSON cache of ESGF queries for is latest check
    --dataset                           Perform test at only the dataset level
    --datafile                          Perform test at only the datafile level
    --is_latest_consistent              Check that the up_to_date status flag of dataset and datafiles is consistent

    This database builder utilises global variables and settings that are defined in qc_settings.py
"""
import django
django.setup()
from qcapp.models import *


import collections
import os
import timeit
import datetime
import time
import re
import glob
import commands
import hashlib
import requests
import itertools
import json as jsn
from subprocess import call
from sys import argv
from docopt import docopt
from ceda_cc import c4
#from cfchecker.cfchecks import CFVersion, CFChecker, newest_version
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from django.db.models import Count, Max, Min, Sum, Avg
from qc_settings import *
from time_checks.run_file_timechecks import main as single_file_time_checks
from time_checks.run_multifile_timechecks import main as multi_file_time_checks
requests.packages.urllib3.disable_warnings()

from utils import *
from esgf_dict import EsgfDict
from cf_checking import run_cf_checker, parse_cf_checker
from cedacc_checking import run_ceda_cc, parse_ceda_cc
from time_checking import file_time_checks
from is_latest import *





def esgf_ds_search(search_template, facet_check, project, variable, table, frequency, experiment, model, node, distrib,
                   latest):
    """

    Performs an ESGF search using a specified search template to query for a list of facets that are valid for
    the given search criteria. E.g. which models exist for facet_check=ensemble, project=CMIP5, variable=tas,
    table=Amon, frequency=mon, experiment=historical, model=HadGEM2-ES,
    node=esgf-index1.ceda.ac.uk, distrib=False, latest=True.
    This example represents a search of the local node only and would return a list of ensemble members which were
    valid for these conditions.

    :param search_template: URL template defined in qc_settings.py
    :param facet_check: The facet to investigate
    :param other parameters are passed in from the global scope which allows % vars()
    :return: Tuple of (list of facets satisfying the search criteria, json response of URL query)
    """

    url = search_template % vars()
    resp = requests.get(url, verify=False)
    json = resp.json()
    result = json["facet_counts"]["facet_fields"][facet_check]
    result = dict(itertools.izip_longest(*[iter(result)] * 2, fillvalue=""))

    return result, json


def create_datafile_records(var, freq, table, expt, node, distrib, latest, project):
    """

    Pre-requisites: Valid qcapp Dataset objcets

    For each of the qcapp Datasets for each of the given criteria (variable, frequency, cmor_table, experiment) an
    ESGF search is performed for datafiles this information is extracted and is used to populate the DataFile
    qcapp table.

    Additionally for each of the datafiles found
    1. The CEDA data archive path is generated from the download url
    2. Is currently restricted to only files that end with ".nc"
        TODO: consider ICHEC_EC-EARTH datafiles that have ".nc4" extensions
        TODO: consider MOHC datafiles that have ".nc_[0-4]" extenstions
    3. Generates the local MD5 sum
    4. Calls is_timeseries to set True/False flag for whether the datafile is part of a timeseries
    5. Generates the DataFile entry in the qcapp.

    :param var: A CP4CDS and CMIP5 valid variable
    :param freq: A CP4CDS and CMIP5 valid frequency
    :param table: A CP4CDS and CMIP5 valid CMOR table
    :param expt: A CP4CDS and CMIP5 valid experiment
    :param node: :distrib: :latest: puts these in the local scope for use with % vars()

    :return:
    """

    for ds in Dataset.objects.filter(variable=var, cmor_table=table, frequency=freq, experiment=expt):
        variable = ds.variable
        table = ds.cmor_table
        frequency = ds.frequency
        experiment = ds.experiment
        model = ds.model
        ensemble = ds.ensemble
        version = ds.version
        project = ds.project

        url = URL_FILE_INFO % vars()
        resp = requests.get(url, verify=False)
        json = resp.json()
        datafiles = json["response"]["docs"]

        for datafile in range(len(datafiles)):
            df = datafiles[datafile]
            ceda_filepath = df["url"][0].split('|')[0].replace("http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/", ARCHIVE_ROOT)

            # Check file exists at datacentre
            if not os.path.basename(ceda_filepath).endswith(".nc"):
                pass
            else:
                # For all valid files
                if not os.path.isfile(ceda_filepath):
                    with open(NO_FILE_LOG, 'a') as fe:
                        fe.write("NOT VALID CEDA FILE: %s" % ceda_filepath)

                # Obtain SHA256 checksum if it exists else place empty entry in the DataFile record
                if df["checksum_type"][0].strip() == "SHA256":
                    sha256_checksum = df["checksum"][0].strip()
                else: sha256_checksum = ""


                start_time, end_time = get_start_end_times(frequency, ceda_filepath)
                md5_checksum = commands.getoutput('md5sum ' + ceda_filepath).split(' ')[0]
                isTimeseries = is_timeseries(ceda_filepath)

                # Create a Datafile record for each file
                newfile, _ = DataFile.objects.get_or_create(dataset=ds,
                                                            archive_path=ceda_filepath,
                                                            ncfile=os.path.basename(ceda_filepath),
                                                            size=df["size"],
                                                            sha256_checksum=sha256_checksum,
                                                            md5_checksum=md5_checksum,
                                                            tracking_id=df["tracking_id"][0].strip(),
                                                            download_url=df["url"][0].strip(),
                                                            variable=variable,
                                                            variable_long_name=df["variable_long_name"][0].strip(),
                                                            cf_standard_name=df["cf_standard_name"][0].strip(),
                                                            variable_units=df["variable_units"][0].strip(),
                                                            start_time=start_time,
                                                            end_time=end_time,
                                                            timeseries=isTimeseries
                                                            )


def create_dataset_records(variable, frequency, table, experiment, node, distrib, latest, spec, project):
    """

    This routine searches ESGF to obtain valid data criteria and then creates a qcapp Dataset record,
    it also then links the Dataset record to the DataSpecification record.


    :param variable: A CP4CDS and CMIP5 valid variable
    :param frequency: A CP4CDS and CMIP5 valid variable
    :param table: A CP4CDS and CMIP5 valid CMOR_table
    :param experiment: A CP4CDS and CMIP5 valid experiment
    :param node: :distrib: :latest: ensures that these are available in the local scope
    :param spec: Link through to the DataSpecification record in qcapp
    :return:
    """

    # Get a dictionary of models that match a given search criteria
    models, json = esgf_ds_search(URL_DS_MODEL_FACETS, 'model', project, variable, table, frequency,
                                  experiment, '', node, distrib, latest)

    for model in models.keys():

        # Get a dictionary of ensemble members that match a given search criteria
        ensembles, json = esgf_ds_search(URL_DS_ENSEMBLE_FACETS, 'ensemble', project, variable, table, frequency,
                                         experiment, model, node, distrib, latest)

        for dset in range(len(json["response"]["docs"])):

            dataset = json["response"]["docs"][dset]

            # Make the dataset record
            ds, _ = Dataset.objects.get_or_create(project=project,
                                                  product=dataset["product"][0].strip(),
                                                  institute=dataset["institute"][0].strip(),
                                                  model=model,
                                                  experiment=experiment,
                                                  frequency=frequency,
                                                  realm=dataset["realm"][0].strip(),
                                                  cmor_table=table,
                                                  ensemble=dataset["ensemble"][0].strip(),
                                                  variable=variable,
                                                  version=dataset["version"].strip(),
                                                  esgf_drs=dataset["drs_id"][0].strip(),
                                                  esgf_node=dataset["data_node"].strip()
                                                  )

            # Link this to the DataSpecification table
            ds.data_spec.add(spec)
            ds.save()


def make_qc_err_record(dfile, checkType, errorType, errorMessage, filepath):
    """

    Make a QCerror record based on file inputs.

    :param dfile: The DataFile record associated with the QCerror
    :param checkType: QC check type
    :param errorType: QC error type
    :param errorMessage: QC error message
    :param filepath: Filepath to QC output, e.g. CEDA-CC record
    :return:
    """

    qc_err, _ = QCerror.objects.get_or_create(file=dfile,
                                              check_type=checkType,
                                              error_type=errorType,
                                              error_msg=errorMessage,
                                              report_filepath=filepath
                                              )


def create_dataspec(requester, variable, frequency, table):
    """

    Creates a DataSpecification record based on input variables

    :param requester: Project making data request
    :param variable: CP4CDS and CMIP5 valid variable
    :param frequency: CP4CDS and CMIP5 valid frequency
    :param table: CP4CDS and CMIP5 valid cmor_table
    :return: DataSpecification table object
    """

    dRequester, _ = DataRequester.objects.get_or_create(requested_by=requester)
    dSpec, _ = DataSpecification.objects.get_or_create(variable=variable, cmor_table=table, frequency=frequency)
    dSpec.datarequesters.add(dRequester)
    dSpec.save()

    return dSpec


def create_records(var, freq, table):
    for expt in ALLEXPTS:
        dspec = create_dataspec(requester, var, freq, table)
        create_dataset_records(var, freq, table, expt, node, distrib, latest, dspec, "CMIP5")
        create_datafile_records(var, freq, table, expt, node, distrib, latest, "CMIP5")


def test(arguments):
    var = arguments['VAR']
    table = arguments['TABLE']
    freq = arguments['FREQ']

    print "Input argument {} is {}".format('VAR', arguments['VAR'])
    print "Input argument {} is {}".format('TABLE', arguments['TABLE'])
    print "Input argument {} is {}".format('FREQ', arguments['FREQ'])


    if arguments['--check_up_to_date'] or \
       arguments['--run_cedacc'] or \
       arguments['--parse_cedacc'] or \
       arguments['--run_cfchecker'] or \
       arguments['--parse_cfchecker'] or \
       arguments['--run_single_file_timechecks'] :

        print "Running tests:"
        for k in arguments.keys():
             if arguments[k]:
                 print k

    else:
        print "Not running any tests all are false"


def main(arguments):

    """

        main

    Main takes the input arguments and parses these into the performing the desired actions

    :param arguments:
    :return:
    """

    var = arguments['VAR']
    table = arguments['TABLE']
    freq = arguments['FREQ']

    esgf_dict = EsgfDict([
        ("node", "esgf-index1.ceda.ac.uk"),
        ("project", "CMIP5"),
        ("institute", None),
        ("model", None),
        ("experiment", None),
        ("frequency", freq),
        ("realm", None),
        ("table", table),
        ("ensemble", None),
        ("variable", var),
        ("ncfile", None),
        ("distrib", None),
        ("latest", None),
    ])

    if arguments['--test']: test(arguments)

    if arguments['--esgf-ds-logger']: esgf_dataset_uptodate_logger(var, table, freq)

    if arguments['--create']: create_records(var, freq, table)

    if arguments['--check_up_to_date'] or arguments['--run_cedacc'] or arguments['--parse_cedacc'] or \
            arguments['--run_cfchecker'] or arguments['--parse_cfchecker'] or arguments['--run_single_file_timechecks']:

        for expt in ALLEXPTS:

            for df in DataFile.objects.filter(dataset__variable=var,
                                              dataset__cmor_table=table,
                                              dataset__frequency=freq,
                                              dataset__experiment=expt
                                              ):
                file = df.archive_path
                if arguments['--check_up_to_date']: up_to_date_check(df, file, var, table, freq, expt)
                if arguments['--run_cedacc']: run_ceda_cc(file)
                if arguments['--parse_cedacc']: parse_ceda_cc(file)
                if arguments['--run_cfchecker']: run_cf_checker(file)
                if arguments['--parse_cfchecker']: parse_cf_checker(file)
                if arguments['--run_single_file_timechecks']: file_time_checks(file)

    if arguments['--run_multi_file_timechecks']:

        for expt in ALLEXPTS:
            dss = Dataset.objects.filter(variable=var, cmor_table=table, frequency=freq, experiment=expt)
            for ds in dss:
                if ds.datafile_set.count() > 1:
                    df = ds.datafile_set.first()
                    dir_of_files = os.path.dirname(df.archive_path)
                    print dir_of_files
                    files = os.listdir(dir_of_files)
                    filelist = []
                    for f in files:
                        filelist.append(os.path.join(dir_of_files, f))

                    multi_file_time_checks(filelist, TS_DIR)

    if arguments['--generate_latest_cache']:
        esgf_dict['distrib'] = True
        esgf_dict['latest'] = True

        for expt in ALLEXPTS:
            esgf_dict['experiment'] = expt
            datasets = Dataset.objects.filter(variable=var, cmor_table=table, frequency=freq, experiment=expt)

            if arguments['--dataset']:
                is_latest_generate_cache(datasets, esgf_dict, "dataset")
            if arguments['--datafile']:
                is_latest_generate_cache(datasets, esgf_dict, "datafile")

    if arguments['--check_data_is_latest']:

        esgf_dict['distrib'] = True
        esgf_dict['latest'] = True

        for expt in ALLEXPTS:
            esgf_dict['experiment'] = expt
            datasets = Dataset.objects.filter(variable=var, cmor_table=table, frequency=freq, experiment=expt)

            # if arguments['--dataset']:
            # dataset_latest_check(datasets, esgf_dict)
            # if arguments['--datafile']:
            check_datafiles_are_latest(datasets, esgf_dict)


    if arguments['--is_latest_consistent']:
        for expt in ALLEXPTS:
            datasets = Dataset.objects.filter(variable=var, cmor_table=table, frequency=freq, experiment=expt)
            check_ds_and_df_is_latest_match(datasets, esgf_dict)

if __name__ == '__main__':

    arguments = docopt(__doc__, version='1.0.0rc2')
    main(arguments)

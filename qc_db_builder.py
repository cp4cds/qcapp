
"""
Usage:
  qc_db_builder.py  [VAR] [TABLE] [FREQ]
                    [--create] [--run_cedacc] [--parse_cedacc] [--run_cfchecker] [--parse_cfchecker]
                    [--check_up_to_date] [--run_single_file_timechecks] [--run_multi_file_timechecks]
                    [--test] [--esgf-ds-logger] [--check_data_is_latest] [--generate_latest_cache ]

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


class EsgfDict(dict):

    def _format_gen_url(self, template, **kwargs):
        return template.format(**kwargs)

    def _generate_local_logdir(self, basedir, ds, edict, subdir=None, rw='r'):

        edict["institute"] = ds.institute
        edict["model"] = ds.model
        edict["realm"] = ds.realm
        edict["ensemble"] = ds.ensemble

        if subdir == None: logdir = basedir
        if subdir == "exper": logdir = os.path.join(basedir, edict["experiment"])

        logfile = ds.esgf_drs + ".json"
        json_file = os.path.join(logdir, logfile)

        if rw == 'w':
            if not os.path.isdir(logdir):
                os.makedirs(logdir)

        return esgf_dict, json_file


    def esgf_query(self, url, logfile):
        resp = requests.get(url, verify=False)
        json = resp.json()
        with open(logfile, 'w') as fw:
            jsn.dump(json, fw)



    def format_is_latest_datafile_url(self):
        template="https://{node}/esg-search/search?type=File&project={project}&institute={institute}&" \
                 "time_frequency={frequency}&realm={realm}&title={ncfile}&distrib={distrib}&latest={latest}" \
                 "&format=application%2Fsolr%2Bjson&limit=10000"

        return self._format_gen_url(template,
                                    node=self['node'],
                                    project=self['project'],
                                    institute=self['institute'],
                                    frequency=self['frequency'],
                                    realm=self['realm'],
                                    ncfile=self['ncfile'],
                                    distrib=self['distrib'],
                                    latest=self['latest'],
                                    )


    def format_is_latest_dataset_url(self):

        template="https://{node}/esg-search/search?type=Dataset&project={project}&institute={institute}&model={model}&" \
                 "experiment={experiment}&time_frequency={frequency}&realm={realm}&cmor_table={table}&ensemble={ensemble}&" \
                 "distrib={distrib}&latest={latest}" \
                 "&format=application%2Fsolr%2Bjson&limit=10000"

        return self._format_gen_url(template,
                                    node=self['node'],
                                    project=self['project'],
                                    institute=self['institute'],
                                    model=self['model'],
                                    experiment=self['experiment'],
                                    frequency=self['frequency'],
                                    realm=self['realm'],
                                    table=self['table'],
                                    ensemble=self['ensemble'],
                                    distrib=self['distrib'],
                                    latest=self['latest'],
                                    )


def file_time_checks(ifile):


    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = ifile.split('/')[6:]
    tc_odir = os.path.join(TC_DIR, institute, model, experiment, frequency, realm, version)

    if not os.path.exists(tc_odir):
        os.makedirs(tc_odir)

    single_file_time_checks(ifile, tc_odir)


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


def run_ceda_cc(file):
    """

    Runs CEDA-CC on the input file

    :param file: valid filepath to run CEDA-CC
    TODO: Check file exits
    TODO: Check CEDA-CC has run ok
    :return:
    """

    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = file.split('/')[6:]

    # Use facets to create directory for CEDA-CC output e.g. BASEDIR/model/experiment/table/<files>
    cedacc_odir = os.path.join(CEDACC_DIR, institute, model, experiment, frequency, realm, version)
    if not os.path.exists(cedacc_odir):
        os.makedirs(cedacc_odir)

    # Run CEDA-CC
    cedacc_args = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', cedacc_odir, '--cae', '--blfmode', 'a']
    run_cedacc = c4.main(cedacc_args)


def parse_ceda_cc(file):
    """

    Parses the CEDA-CC output on the input file.

    Finds any errors recorded by CEDA-CC and then makes a QCerror record for each found.

    :param file: Archive file
    TODO: check it is a valid file?
    :return:
    """

    checkType = "CEDA-CC"
    temporal_range = file.split("_")[-1].strip(".nc").split("_")[0]
    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = file.split('/')[6:]
    file_base = "_".join([variable, table, model, experiment, ensemble, temporal_range])

    # Constructs a CEDA-CC regex based on variable_table_model_experiment_ensemble_temporal-range__qclog_{date}.txt
    ceda_cc_file_pattern = re.compile(file_base + "__qclog_\d+\.txt")

    # List files in the CEDA-CC logdir
    log_dir = os.path.join(CEDACC_DIR,  institute, model, experiment, frequency, realm, version)
    log_dir_files = os.listdir(log_dir)

    for logfile in log_dir_files:

        # If the input file is in the logdir parse the output
        if ceda_cc_file_pattern.match(logfile):
            ceda_cc_file = os.path.join(log_dir, logfile)
            with open(ceda_cc_file, 'r') as fr:
                ceda_cc_out = fr.readlines()

            # Identify where CEDA-CC picks up a QC error
            cedacc_global_error = re.compile('.*global.*FAILED::.*')
            cedacc_variable_error = re.compile('.*variable.*FAILED::.*')
            cedacc_other_error = re.compile('.*filename.*FAILED::.*')
            cedacc_exception = re.compile('.*Exception.*')
            cedacc_abort = re.compile('.*aborted.*')

            # For CEDA-CC ouput search for errors and if found make a QCerror record
            for line in ceda_cc_out:
                if cedacc_global_error.match(line.strip()):
                    make_qc_err_record(df, checkType, "global", line, ceda_cc_file)
                if cedacc_variable_error.match(line.strip()):
                    make_qc_err_record(df, checkType, "variable", line, ceda_cc_file)
                if cedacc_other_error.match(line.strip()):
                    make_qc_err_record(df, checkType, "other", line, ceda_cc_file)
                if cedacc_exception.match(line.strip()):
                    make_qc_err_record(df, checkType, "fatal", line, ceda_cc_file)
                if cedacc_abort.match(line.strip()):
                    make_qc_err_record(df, checkType, "fatal", line, ceda_cc_file)


def run_cf_checker(file):
    """

    Run the CF-Checker on the input file from the shell by calling out using subprocess.call

    :param file: Archive NetCDF file
    TODO: validate input file
    :return:
    """
    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = file.split('/')[6:]

    # Make a CF output directory
    cf_odir = os.path.join(CF_DIR, institute, model, experiment, frequency, realm, version)
    if not os.path.exists(cf_odir):
        os.makedirs(cf_odir)

    # Define output and error log files
    cf_out_file = os.path.join(cf_odir, ncfile.replace(".nc", ".cf-log.txt"))
    cf_err_file = os.path.join(cf_odir, ncfile.replace(".nc", ".cf-err.txt"))
    run_cmd = ["/usr/bin/cf-checker", "-a", AREATABLE, "-s", STDNAMETABLE, "-v", "auto", file]

    cf_out, cf_err = open(cf_out_file, "w"), open(cf_err_file, "w")
    call(run_cmd, stdout=cf_out, stderr=cf_err)
    cf_out.close(), cf_err.close()

    if os.path.getsize(cf_err_file) == 0:
        os.remove(cf_err_file)
    else:
        filen = file.replace('/', '.') + '.cf-err'
        filename = os.path.join(CF_FATAL_DIR, filen)
        touch_cmd = ["touch", filename]
        call(touch_cmd)


def parse_cf_checker(file):
    """

    Parses the CF-Checker output for the input file

    Finds any errors recorded by the CF-Checker and then makes a QCerror record for each found.

    :param file: Archive file
    TODO: check it is a valid file?

    :return:
    """

    checkType = "CF"

    temporal_range = file.split("_")[-1].strip(".nc").split("_")[0]
    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = file.split('/')[6:]
    file_base = "_".join([variable, table, model, experiment, ensemble, temporal_range])

    # Constructs a CF file regex based on variable_table_model_experiment_ensemble_temporal-range.cf-log.txt
    cf_file_pattern = re.compile(file_base + ".cf-log.txt")

    # List files in the CF logdir
    log_dir = os.path.join(CF_DIR, institute, model, experiment, frequency, realm, version)
    log_dir_files = os.listdir(log_dir)

    for logfile in log_dir_files:

        # If the input file is in the logdir parse the output
        if cf_file_pattern.match(logfile):
            with open(os.path.join(log_dir, logfile), 'r') as fr:
                cf_out = fr.readlines()

            # CF regex expressions for errors
            cf_global_error = re.compile('.*ERROR.*(global|Global|Convention).*')
            cf_variable_error = re.compile('.*ERROR.*(units|cell).*(?!.*(time|boundary|coordinate)).*variable.*')
            cf_other_error = re.compile('.*ERROR.*(bound|Boundary|grid|coordinate|dimension).*')
            cf_abort = re.compile('.*suffix.*')

            # Dictionary mapping the CF regex with type of error
            regexlist = [(cf_global_error, "global"),
                         (cf_variable_error, "variable"),
                         (cf_other_error, "other"),
                         (cf_abort, "fatal")]

            # Identify where CF picks up a QC error
            for line in cf_out:
                for regex, label in regexlist:
                    if regex.match(line.strip()):
                        make_qc_err_record(df, checkType, label, line, os.path.join(log_dir, logfile))


    # TODO: Must add in a test for a non-zero .cf-err.txt and record perhaps retry or read in only here


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


def check_cfout():
    """

    Checks the CF output for erroneous *.cf-err.txt files
    If a *.cf-err.txt file exists then the CF checker is re-run to ensure that the output is not erroneous.

    TODO this needs to be integrated into the main CF-Checking routines

    This does not run in parallel context only a debugging function

    """
    basedir = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/CF-OUTPUT'
    institutes = os.listdir(basedir)
    for i in institutes:
        expts = os.listdir(os.path.join(basedir, i))
        for e in expts:
            realms = os.listdir(os.path.join(basedir, i, e))
            for r in realms:
                for f in os.listdir(os.path.join(basedir, i, e, r)):
                    if f.endswith('cf-err.txt'):
                        if os.path.getsize(os.path.join(basedir, i, e, r, f)) != 0:
                            print os.path.join(basedir, i, e, r, f)
                            err_file = os.path.join(basedir, i, e, r, f)
                            log_file = os.path.join(basedir, i, e, r, f.replace("-err", "-log"))
                            if os.path.getsize(log_file) != 0:
                                with open(log_file, 'r') as reader:
                                    data = reader.readlines()
                                    datafile = data[1].strip('\n').strip('CHECKING NetCDF FILE: ')
                                    print datafile
                                    run_cf_checker(datafile)
                            else:
                                with open('fatal_no_cf_checks.log', 'a') as elog:
                                    elog.writelines([err_file, '\n'])


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


def create_records(var, freq, table):
    for expt in ALLEXPTS:
        dspec = create_dataspec(requester, var, freq, table)
        create_dataset_records(var, freq, table, expt, node, distrib, latest, dspec, "CMIP5")
        create_datafile_records(var, freq, table, expt, node, distrib, latest, "CMIP5")


def up_to_date_check(df, file, variable, table, frequency, experiment):
    """

    Checks whether the archive file is the most recent version.

    :param df: DataFile object
    :param file: archive file
    TODO: validate file?
    :param variable: CP4CDS and CMIP5 valid variable
    :param table: CP4CDS and CMIP5 valid cmor table
    :param frequency: CP4CDS and CMIP5 valid frequency
    :param experiment: CP4CDS and CMIP5 valid experiment
    :return:
    """

    uptodate, uptodateNotes = is_latest_version(file, variable, table, frequency, experiment,
                                                df.dataset.model,
                                                df.dataset.ensemble,
                                                df.dataset.version,
                                                df.md5_checksum,
                                                df.sha256_checksum
                                                )

    df.up_to_date = uptodate
    df.up_to_date_note = uptodateNotes
    df.save()


def is_latest_version(archive_path, variable, table, frequency, experiment, model, ensemble,
                      version, md5_checksum, sha256_checksum):
    """

    Checks if file is the latest version by performing an ESGF search

    :param archive_path: full archive filepath
    :param variable: CP4CDS and CMIP5 valid variable
    :param table: CP4CDS and CMIP5 valid cmor table
    :param frequency: CP4CDS and CMIP5 valid frequency
    :param experiment: CP4CDS and CMIP5 valid experiment
    :param model: CP4CDS and CMIP5 valid model
    :param ensemble: CP4CDS and CMIP5 valid ensemble
    :param version:
    :param md5_checksum:
    :param sha256_checksum:
    :return: Tuple of uptodate [Boolean] and uptodateNote [string]
    """

    distrib_latest = True
    replica_latest = False
    version = "v" + version

    # puts these variables in the local scope
    # TODO tidy this up
    latest_node = "esgf-index1.ceda.ac.uk"
    latest_project = 'CMIP5'
    latest_latest = True

    # Perform a distributed ESGF search for the archive file, where replica=False, latest=True
    url = URL_LATEST_TEMPLATE % vars()
    resp = requests.get(url, verify=False)
    json = resp.json()

    if json["response"]["numFound"] == 0:
        uptodate = False
        uptodateNotes = "NO URL RESPONSE: %s" % url
        return uptodate, uptodateNotes

    else:

        for resp in range(len(json["response"]["docs"])):
            json_resp = json["response"]["docs"][resp]

            if json_resp["title"] == os.path.basename(archive_path):
                id = json_resp["id"].strip()
                checksum = json_resp["checksum"][0].strip()
                checksum_type = json_resp["checksum_type"][0].strip()
                datanode = id.split('|')[1]
                dataset_id = id.split('|')[0]
                latest_version = dataset_id.split('.')[-3]

                if checksum_type == "MD5":
                    checksum_match = checksum == md5_checksum
                else:
                    checksum_match = checksum == sha256_checksum

                if checksum_match:
                    uptodate = True
                    uptodateNotes = "UP TO DATE"
                    return uptodate, uptodateNotes
                else:
                    uptodate = False
                    if latest_version != version:
                        uptodateNotes = "VERSION MISMATCH. Old version: %s, latest version %s, url %s" % \
                                        (version, latest_version, url)
                        return uptodate, uptodateNotes
                    else:
                        uptodateNotes = "UNKNOWN: Checksums don't match unknown reason, %s" % url
                        return uptodate, uptodateNotes
            else:
                uptodate = False
                uptodateNotes = "NO MATCHING FILE FOUND: %s" % url
                return uptodate, uptodateNotes








def is_latest_dataset_cache(datasets, variable, esgf_dict):

    for ds in datasets:
        esgf_dict, json_file = esgf_dict._generate_local_logdir(DATASET_LATEST_CACHE, ds, esgf_dict, subdir=None, rw='w')
        url = esgf_dict.format_is_latest_dataset_url()
        esgf_dict.esgf_query(url, json_file)


def is_latest_datafile_cache(datasets, variable, esgf_dict):

    for ds in datasets:
        esgf_dict, json_file = esgf_dict._generate_local_logdir(DATAFILE_LATEST_CACHE, ds, esgf_dict, subdir="exper", rw='w')
        dfs = ds.datafile_set.all()

        for df in dfs:
            esgf_dict["ncfile"] = df.ncfile
            url = esgf_dict.format_is_latest_datafile_url()
            esgf_dict.esgf_query(url, json_file)



def get_all_versions(json_resp, versions, logfile, type):

    for d in json_resp:
        dataset_id = d["id"].split('|')[0]
        with open(logfile, 'w') as fw:
            fw.writelines("Checking {} is up to date :: {} \n".format(type, dataset_id))

        data_node = d["id"].split('|')[1]

        versions[data_node] = d["id"].split('|')[0].split('.')[-1].strip('v')

    return versions


def get_all_checksums(json_resp, cksums, logfile, type):

    for d in json_resp:
        dataset_id = d["id"].split('|')[0]
        with open(logfile, 'w') as fw:
            fw.writelines("Checking {} is up to date :: {} \n".format(type, dataset_id))

        data_node = d["id"].split('|')[1]
        version = d["dataset_id"].split('|')[0].split('.')[-1].strip('v')
        try:
            cksum = d["checksum"][0].strip()
        except KeyError:
            cksum = d["checksum"]
        cksum_type = d["checksum_type"][0].strip()
        replica = d["replica"]

        cksums[data_node] = {'replica': replica, 'version': version, 'cksum_type': cksum_type, 'cksum': cksum}

    return cksums




def convert_version(iversion):
    if len(iversion) == 8:
        oversion = datetime.datetime(int(iversion[0:4]), int(iversion[4:6]), int(iversion[6:8]))
    if len(iversion) == 1:
        oversion = iversion

    return oversion


def get_latest_version(db_obj, versions, logfile):
    # Get latest version/ Handles both v<YYYYMMDD> and v<N> formats

    dt_versions = []
    for version in versions:
        dt_versions.append(convert_version(version))

    try:
        latest_version = max(dt_versions)
        valid_latest_version = True

    except TypeError:
        errmsg = "LATEST.006 [FATAL] :: Cannot perform version_qc, no known latest version " \
                 "as types do not match {} \n".format(versions)
        db_obj.up_to_date_note = errmsg
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(errmsg))
        latest_version = None
        valid_latest_version = False

    return valid_latest_version, latest_version


def _def_latest_checksum_dict(node, version, cksum_type, cksum):
    cksum_dict = {}
    cksum_dict['node'] = node
    cksum_dict['version'] = version
    cksum_dict['cksum_type'] = cksum_type
    cksum_dict['cksum'] = cksum

    return cksum_dict

def get_latest_checksum(db_obj, cksums, logfile):
    """
    cksums[data_node] = {'replica': replica, 'version': version, 'cksum_type': cksum_type, 'cksum': cksum}

    :param db_obj:
    :param cksums:
    :param logfile:
    :return:
    """


    latest_checksum = {}
    versions = []

    for key, values in cksums.items():
        if values["replica"] == False: # i.e. master record
            latest_checksum = _def_latest_checksum_dict(key, values['version'], values['cksum_type'], values['cksum'])
            valid_latest_checksum = True
            return valid_latest_checksum, latest_checksum

        versions.append(values['version'])

    valid_latest_checksum, latest_cksum = get_latest_version(db_obj, versions, logfile)
    if isinstance(latest_cksum, datetime.datetime): latest_cksum = latest_cksum.strftime('%Y%m%d')

    if valid_latest_checksum:
        for key, values in cksums.items():
            if values['version'] == latest_cksum and values['cksum_type']=='SHA256':
               latest_checksum = _def_latest_checksum_dict(key, values['version'], values['cksum_type'], values['cksum'])
               return valid_latest_checksum, latest_checksum
        for key, values in cksums.items():
            if values['version'] == latest_cksum:
               latest_checksum = _def_latest_checksum_dict(key, values['version'], values['cksum_type'], values['cksum'])
               return valid_latest_checksum, latest_checksum
    else:
        errmsg = "LATEST.009 [FATAL] :: Cannot determine latest checksum in get_latest_checksum"
        db_obj.up_to_date_note = errmsg
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(errmsg))
        valid_latest_checksum = False
        latest_checksum = _def_latest_checksum_dict(None, None, None, None)
        return valid_latest_checksum, latest_checksum




def _check_published_and_db_versions_match(db_obj, ceda_publish_version_no, ceda_database_version_no, logfile):
    try:
        if ceda_database_version_no == ceda_publish_version_no:
            logmsg = "LATEST.003 [PASS] :: MATCH - CEDA database version {} and published " \
                     "ESGF version {} are the same".format(ceda_database_version_no, ceda_publish_version_no)
            with open(logfile, 'a') as fw: fw.writelines("{} \n".format(logmsg))
            return True

        if ceda_database_version_no != ceda_publish_version_no:
            errmsg = "LATEST.003 [ERROR] :: Mismatch between CEDA database version {} and " \
                     "ESGF version {}".format(ceda_database_version_no, ceda_publish_version_no)
            db_obj.up_to_date_note = errmsg
            with open(logfile, 'a') as fw: fw.writelines("{} \n".format(errmsg))

            return False

    except AttributeError:
        errmsg = "LATEST.004 [ERROR] :: CEDA database version unspecified"
        db_obj.up_to_date_note = errmsg
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(errmsg))

        return False



def _check_published_and_db_checksums_match(db_obj, ceda_published_cksum, ceda_database_cksum, logfile):
    try:
        if ceda_database_cksum == ceda_published_cksum:
            logmsg = "LATEST.003 [PASS] :: MATCH - CEDA database version {} and published " \
                     "ESGF version {} are the same".format(ceda_database_cksum, ceda_published_cksum)
            with open(logfile, 'a') as fw: fw.writelines("{} \n".format(logmsg))
            return True

        if ceda_database_cksum != ceda_published_cksum:
            errmsg = "LATEST.003 [ERROR] :: Mismatch between CEDA database version {} and " \
                     "ESGF version {}".format(ceda_database_cksum, ceda_published_cksum)
            db_obj.up_to_date_note = errmsg
            with open(logfile, 'a') as fw: fw.writelines("{} \n".format(errmsg))

            return False

    except AttributeError:
        errmsg = "LATEST.004 [ERROR] :: CEDA database version unspecified"
        db_obj.up_to_date_note = errmsg
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(errmsg))

        return False

def check_dataset_version(db_obj, versions, latest_version, ceda_data_node, logfile):
    ceda_published_version_no = versions[ceda_data_node]
    ceda_database_version_no = db_obj.version

    is_match_ceda_versions = _check_published_and_db_versions_match(db_obj, ceda_published_version_no,
                                                                    ceda_database_version_no, logfile)

    if is_match_ceda_versions:
        ceda_version = convert_version(ceda_published_version_no)
        ceda_version_is_latest = compare_ceda_with_latest_version(db_obj, ceda_version, latest_version, logfile)


def check_datafile_version(db_obj, all_cksums, latest_cksum, ceda_data_node, logfile):
    """

    :param db_obj:
    :param all_cksums: {{{'node': {'replica': Boolean, 'cksum_type': 'checksum type', 'version': 'version', 'cksum': 'checksum'}}
    :param latest_cksums: {'node': 'node', 'version': 'version', 'cksum_type': 'cheksum type', 'cksum': 'checksum'}
    :param ceda_data_node:
    :param logfile:
    :return:
    """
    ceda_published_checksum = all_cksums[ceda_data_node]['cksum']
    ceda_database_checksum = db_obj.sha256_checksum

    is_match_ceda_cksums = _check_published_and_db_checksums_match(db_obj, ceda_published_checksum,
                                                                     ceda_database_checksum, logfile)


    if latest_cksum['cksum_type'] == "md5":
        ceda_database_checksum = db_obj.md5_checksum

    if is_match_ceda_cksums:
        ceda_cksum_is_latest = compare_ceda_with_latest_cksum(db_obj, ceda_database_checksum, latest_cksum['cksum'], logfile)
        return ceda_cksum_is_latest
    else:
        ceda_cksum_is_latest = False
        return ceda_cksum_is_latest


def compare_ceda_with_latest_cksum(db_obj, ceda_version, latest_version, logfile):

    if ceda_version == latest_version:
        logmsg = "LATEST.000 [PASS] :: CEDA version is up to date at version: {}".format(latest_version)
        db_obj.up_to_date = True
        db_obj.up_to_date_note = logmsg
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(logmsg))
        return True


    if ceda_version != latest_version:
        errmsg = "LATEST.002 [ERROR] :: CEDA version is out of date. CEDA version is: {}, " \
                 "LATEST version is: {}".format(ceda_version, latest_version)
        db_obj.up_to_date_note = errmsg
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(errmsg))
        return False


def compare_ceda_with_latest_version(db_obj, ceda_version, latest_version, logfile):
    if ceda_version < latest_version:
        if isinstance(ceda_version, datetime.datetime): ceda_version = ceda_version.strftime("%Y%m%d")
        if isinstance(latest_version, datetime.datetime): latest_version = latest_version.strftime("%Y%m%d")

        errmsg = "LATEST.002 [ERROR] :: CEDA version is out of date. CEDA version is: {}, " \
                 "LATEST version is: {}".format(ceda_version, latest_version)
        db_obj.up_to_date_note = errmsg
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(errmsg))

        return False

    if ceda_version == latest_version:
        if isinstance(ceda_version, datetime.datetime): ceda_version = ceda_version.strftime("%Y%m%d")
        if isinstance(latest_version, datetime.datetime): latest_version = latest_version.strftime("%Y%m%d")

        errmsg = "LATEST.000 [PASS] :: CEDA version is up to date at version: {}".format(latest_version)
        db_obj.up_to_date = True
        db_obj.up_to_date_note = errmsg
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(errmsg))
        return True

    if ceda_version > latest_version:
        if isinstance(ceda_version, datetime.datetime):
            ceda_version = ceda_version.strftime("%Y%m%d")
        if isinstance(latest_version, datetime.datetime):
            latest_version = latest_version.strftime("%Y%m%d")

        errmsg = "LATEST.007 [FATAL] :: CEDA version {} can not be greater than " \
                 "latest version: {} \n".format(ceda_version, latest_version)
        db_obj.up_to_date_note = errmsg
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(errmsg))
        return False


def dataset_latest_check(datasets, variable, esgf_dict):

    ceda_data_node = "esgf-data1.ceda.ac.uk"
    version_qc = False

    for ds in datasets:

        # Set up_to_date to be False as default will be overwritten to true if found to be true
        ds.up_to_date = False

        # Open and read cached JSON file
        esgf_dict, json_file = esgf_dict._generate_local_logdir(DATASET_LATEST_CACHE, ds, esgf_dict, subdir=None, rw='r')
        json_data = open(json_file).read()
        _data = jsn.loads(json_data)
        json_resp = _data["response"]["docs"]


        logfile = os.path.join(DATASET_LATEST_DIR, os.path.basename(json_file).replace(".json", ".dataset.log"))

        # versions is a dictionary where the key is the datanode and value is the published version
        versions = {}
        versions = get_all_versions(json_resp, versions, logfile, type="dataset")

        if ceda_data_node not in versions.keys():
            errmsg = "LATEST.001 [ERROR] :: Dataset is missing from CEDA archive"
            with open(logfile, 'a+') as fw: fw.writelines(" {} \n".format(errmsg))
        else:
            valid_latest_version, latest_version = get_latest_version(ds, versions, logfile)
            if valid_latest_version:
                check_dataset_version(ds, versions, latest_version, ceda_data_node, logfile)



def datafile_latest_check(dataset, variable, esgf_dict):
    """
    For a given dataset test all datafiles are latest

    :return:
    """
    ceda_data_node = "esgf-data1.ceda.ac.uk"
    version_qc = False

    for ds in datasets:

        dfs = ds.datafile_set.all()

        for df in dfs:
            df.up_to_date = False
            # Open and read cached JSON file
            esgf_dict, json_file = esgf_dict._generate_local_logdir(DATAFILE_LATEST_CACHE, ds, esgf_dict, subdir="exper", rw='r')
            json_data = open(json_file).read()
            _data = jsn.loads(json_data)
            json_resp = _data["response"]["docs"]
            logfile = os.path.join(DATAFILE_LATEST_DIR, os.path.basename(json_file).replace(".json", ".datafile.log"))

            print json_file

            # versions is a dictionary where the key is the datanode and value is the published version
            checksums = {}
            checksums = get_all_checksums(json_resp, checksums, logfile, type="datafile")
            if ceda_data_node in checksums.keys():
                valid_latest_datafile, latest_checksum = get_latest_checksum(df, checksums, logfile)
                if valid_latest_datafile:
                    ceda_cksum_is_latest = check_datafile_version(df, checksums, latest_checksum, ceda_data_node, logfile)
                else:
                    errmsg = "LATEST.009 [ERROR] :: No latest datafile found"
                    with open(logfile, 'a+') as fw: fw.writelines(" {} \n".format(errmsg))
            else:
                errmsg = "LATEST.001 [ERROR] :: Datafile is missing from CEDA archive"
                with open(logfile, 'a+') as fw: fw.writelines(" {} \n".format(errmsg))
                ceda_cksum_is_latest = False

            if ceda_cksum_is_latest:
                df.up_to_date = True
                logmsg = "LATEST.000 [PASS] :: CEDA datafile is up to date \n " \
                         "CEDA checksum :: {} \n " \
                         "LATEST checksum {} \n " \
                         "LATEST source {}".format(checksums[ceda_data_node]['cksum'],
                                                   latest_checksum['cksum'],
                                                   latest_checksum['node']
                                                   )
                with open(logfile, 'a+') as fw: fw.writelines(" {} \n".format(logmsg))
                print "SUCCESS a valid datafile was found for {}".format(df.ncfile)
            else:
                logmsg = "LATEST.009 [FAIL] :: CEDA datafile is not latest version \n " \
                         "CEDA checksum :: {} \n " \
                         "LATEST checksum {} \n " \
                         "LATEST source {}".format(checksums[ceda_data_node]['cksum'],
                                                   latest_checksum['cksum'],
                                                   latest_checksum['node']
                                                   )
                with open(logfile, 'a+') as fw: fw.writelines(" {} \n".format(logmsg))
                print "FAIL a valid datafile was not found or is missing for {}".format(df.ncfile)




if __name__ == '__main__':

    arguments = docopt(__doc__, version='1.0.0rc2')
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


    if arguments['--check_data_is_latest']:

        esgf_dict['distrib'] = True
        esgf_dict['latest'] = True

        for expt in ['rcp26']:
            esgf_dict['experiment'] = expt
            datasets = Dataset.objects.filter(variable=var, cmor_table=table, frequency=freq, experiment=expt)

            if arguments['--generate_latest_cache']:
                is_latest_dataset_cache(datasets, var, esgf_dict)
                is_latest_datafile_cache(datasets, var, esgf_dict)

#            dataset_latest_check(datasets, var, esgf_dict)
            datafile_latest_check(datasets, var, esgf_dict)
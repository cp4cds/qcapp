
import django
django.setup()
from qcapp.models import *

import collections, os, timeit, datetime, time, re, glob
import commands
import hashlib
import requests, itertools
from subprocess import call
from sys import argv
from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, newest_version
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from django.db.models import Count, Max, Min, Sum, Avg
requests.packages.urllib3.disable_warnings()
from qc_settings import *

project = 'CMIP5'


def is_timeseries(filepath):

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


def esgf_ds_search(search_template, facet_check, project, variable, table, frequency, experiment, model, node, distrib,
                   latest):
    """
    Perform an esgf dataset search using the specified template
    :return: dictionary of facets
    """
    url = search_template % vars()
    if DEBUG: print "DS SEARCH URL:: \n", url
    resp = requests.get(url, verify=False)
    json = resp.json()
    result = json["facet_counts"]["facet_fields"][facet_check]
    result = dict(itertools.izip_longest(*[iter(result)] * 2, fillvalue=""))

    return result, json


def create_datafile_records(var, freq, table, expt, node, distrib, latest):

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
            ceda_filepath = df["url"][0].split('|')[0].\
                replace("http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/", ARCHIVE_ROOT)

            if not os.path.basename(ceda_filepath).endswith(".nc"):
                pass
            else:
                # Check file exists at ceda
                if DEBUG: print ceda_filepath
                if not os.path.isfile(ceda_filepath):
                    if DEBUG: print "FILE DOES NOT EXIST AT CEDA:: ", ceda_filepath
                    with open(NO_FILE_LOG, 'a') as fe:
                        fe.write("NOT VALID CEDA FILE: %s" % ceda_filepath)

                start_time, end_time = get_start_end_times(frequency, ceda_filepath)
                md5_checksum = commands.getoutput('md5sum ' + ceda_filepath).split(' ')[0]

                if df["checksum_type"][0].strip() == "SHA256":
                    sha256_checksum = df["checksum"][0].strip()
                else: sha256_checksum = ""

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


def create_dataset_records(variable, frequency, table, experiment, node, spec):
    """

    :return:
    """
    distrib = False
    latest = True

    # Get a dictionary of models that match a given search criteria
    models, json = esgf_ds_search(URL_DS_MODEL_FACETS, 'model', project, variable, table, frequency,
                                  experiment, '', node, distrib, latest)

    for model in models.keys():

        # Get a dictionary of ensemble members that match a given search criteria
        ensembles, json = esgf_ds_search(URL_DS_ENSEMBLE_FACETS, 'ensemble', project, variable, table, frequency,
                                         experiment, model, node, distrib, latest)

        for dset in range(len(json["response"]["docs"])):

            dataset = json["response"]["docs"][dset]

            if DEBUG:
                print project, dataset["product"][0].strip(), dataset["institute"][0].strip(), model, \
                      experiment, frequency, dataset["realm"][0].strip(), table, dataset["ensemble"][0].strip(), \
                      variable, dataset["version"].strip()

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

    if DEBUG: print file

    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = file.split('/')[6:]
    cedacc_odir = os.path.join(CEDACC_DIR, model, experiment, table)

    if not os.path.exists(cedacc_odir):
        os.makedirs(cedacc_odir)
    cedacc_args = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', cedacc_odir, '--cae', '--blfmode', 'a']
    run_cedacc = c4.main(cedacc_args)

    # Tidy up; move ceda-cc output files to a log_dir
    # cedacc_ofiles = ["cccc_atMapLog.txt",
    #                  "Rec.json",
    #                  "Rec.txt"]
    # for f in cedacc_ofiles:
    #     mv_cmd = ['mv', f, 'log_dir/']
    #     res = call(mv_cmd)


def parse_ceda_cc(file):

    if DEBUG: print file
    checkType = "CEDA-CC"
    if DEBUG: print checkType
    temporal_range = file.split("_")[-1].strip(".nc").split("_")[0]
    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = file.split('/')[6:]

    file_base = "_".join([variable, table, model, experiment, ensemble, temporal_range])
    ceda_cc_file_pattern = re.compile(file_base + "__qclog_\d+\.txt")
    log_dir = os.path.join(CEDACC_DIR, model, experiment, table)
    log_dir_files = os.listdir(log_dir)

    for logfile in log_dir_files:
        if ceda_cc_file_pattern.match(logfile):
            ceda_cc_file = os.path.join(log_dir, logfile)
            if DEBUG: print ceda_cc_file
            with open(ceda_cc_file, 'r') as fr:
                ceda_cc_out = fr.readlines()

            # Identify where CEDA-CC picks up a QC error
            cedacc_global_error = re.compile('.*global.*FAILED::.*')
            cedacc_variable_error = re.compile('.*variable.*FAILED::.*')
            cedacc_other_error = re.compile('.*filename.*FAILED::.*')
            cedacc_exception = re.compile('.*Exception.*')
            cedacc_abort = re.compile('.*aborted.*')

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

    if DEBUG: print file

    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = file.split('/')[6:]
    cf_odir = os.path.join(CF_DIR, model, experiment, table)


    if not os.path.exists(cf_odir):
        os.makedirs(cf_odir)

    cf_out_file = os.path.join(cf_odir, ncfile.replace(".nc", ".cf-log.txt"))
    cf_err_file = os.path.join(cf_odir, ncfile.replace(".nc", ".cf-err.txt"))

    run_cmd = ["cf-checker", "-a", AREATABLE, "-s", STDNAMETABLE, "-v", "auto", file]
    cf_out = open(cf_out_file, "w")
    cf_err = open(cf_err_file, "w")
    call(run_cmd, stdout=cf_out, stderr=cf_err)
    cf_out.close()
    cf_err.close()


def parse_cf_checker(file):

    checkType = "CF"
    if DEBUG: print checkType, file

    temporal_range = file.split("_")[-1].strip(".nc").split("_")[0]
    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = file.split('/')[6:]

    file_base = "_".join([variable, table, model, experiment, ensemble, temporal_range])
    cf_file_pattern = re.compile(file_base + ".cf-log.txt")
    log_dir = os.path.join(CF_DIR, model, experiment, table)
    log_dir_files = os.listdir(log_dir)
    if DEBUG: print file_base, log_dir

    for logfile in log_dir_files:
        if cf_file_pattern.match(logfile):
            if DEBUG: print logfile
            with open(os.path.join(log_dir, logfile), 'r') as fr:
                cf_out = fr.readlines()

            # Identify where CF picks up a QC error

            cf_global_error = re.compile('.*ERROR.*(global|Global|Convention).*')
            cf_variable_error = re.compile('.*ERROR.*(units|cell).*(?!.*(time|boundary|coordinate)).*variable.*')
            cf_other_error = re.compile('.*ERROR.*(bound|Boundary|grid|coordinate|dimension).*')
            cf_abort = re.compile('.*suffix.*')

            regexlist = [(cf_global_error, "global"),
                         (cf_variable_error, "variable"),
                         (cf_other_error, "other"),
                         (cf_abort, "fatal")]

            for line in cf_out:
                for regex, label in regexlist:
                    if regex.match(line.strip()):
                        make_qc_err_record(df, checkType, label, line, os.path.join(log_dir, logfile))


def make_qc_err_record(dfile, checkType, errorType, errorMessage, filepath):

    qc_err, _ = QCerror.objects.get_or_create(file=dfile,
                                              check_type=checkType,
                                              error_type=errorType,
                                              error_msg=errorMessage,
                                              report_filepath=filepath
                                              )


def create_dataspec(requester, variable, frequency, table):
    dRequester, _ = DataRequester.objects.get_or_create(requested_by=requester)
    dSpec, _ = DataSpecification.objects.get_or_create(variable=variable, cmor_table=table, frequency=frequency)
    dSpec.datarequesters.add(dRequester)
    dSpec.save()

    return dSpec


def generate_filelist(FILELIST):

    with open(FILELIST, 'w') as fw:
        for df in DataFiles.objects.all():
            fw.writelines([df.archive_path, "\n"])


def up_to_date_check(df, file, variable, table, frequency, experiment):


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

    distrib_latest = True
    replica_latest = False
    version = "v" + version
    latest_node = node
    latest_project = project
    latest_latest = latest

    url = URL_LATEST_TEMPLATE % vars()
    if DEBUG: print url
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

def check_cfout():

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
                            with open(os.path.join(basedir, i, e, r, f.replace("-err", "-log"))) as reader:
                                data = reader.readlines()
                                datafile = data[1].strip('\n').strip('CHECKING NetCDF FILE: ')
                                print datafile
                                run_cf_checker(datafile)


if __name__ == '__main__':

    """
    Global variables and settings are defined in qc_settings.py
    """

    var = argv[1]
    table = argv[2]
    freq = argv[3]
    # expt = argv[4]
    CREATE = False
    QC = True
    #experiments = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
    experiments = ['historical']

    if DEBUG: print var, freq, table

    # if DEBUG:
    #     check_cfout()
    #     asdfsd
    # asdfasd

    if CREATE:
        if DEBUG: print "creating"
        for expt in experiments:
            dspec = create_dataspec(requester, var, freq, table)
            create_dataset_records(var, freq, table, expt, node, dspec)
            create_datafile_records(var, freq, table, expt, node, distrib, latest)

    if QC:
        if DEBUG: print "running QC"

        for expt in experiments:
            if DEBUG: print expt

            for df in DataFile.objects.filter(dataset__variable=var,
                                              dataset__cmor_table=table,
                                              dataset__frequency=freq,
                                              dataset__experiment=expt
                                              ):
                file = df.archive_path

                # up_to_date_check(df, file, var, table, freq, expt)
                # run_ceda_cc(file)
                # run_cf_checker(file)

                parse_ceda_cc(file)
                parse_cf_checker(file)
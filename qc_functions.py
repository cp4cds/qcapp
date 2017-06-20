import django

django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

import collections, os, timeit, datetime, time, re, glob
import commands
import requests, itertools

from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version

ARCHIVE_ROOT = "/badc/cmip5/data/"
CEDACC_DIR = "/group_workspaces/jasmin/cp4cds1/qc/QCchecks/CEDACC-OUTPUT"
CFDIR = "/group_workspaces/jasmin/cp4cds1/qc/QCchecks/CF-OUTPUT/"

def read_cf_files(dfile, qcfile):

    checkType = "CF"
    temporal_range = qcfile.split("_")[-1].strip(".nc").split("_")[0]
    institute, model, experiment, frequency, realm, table, ensemble, version, variable, \
    ncfile = qcfile.split('/')[6:]
    log_dir = os.path.join(CFDIR, institute, model, experiment, table, version)
    logfile = 'missing_dirs_cf.log'

    if os.path.exists(log_dir):

        file_list = os.listdir(log_dir)
        file_base = "_".join([variable, table, model, experiment, ensemble])
        file_pattern = re.compile(file_base + "_" + temporal_range + ".cf-log")

        for file in file_list:
            if file_pattern.match(file):
                with open(os.path.join(log_dir, file), 'r') as fr:
                    cf_out = fr.readlines()

                # Identify where CF picks up a QC error

                cf_global_error = re.compile('.*ERROR.*(global|Global|Convention).*')
                cf_variable_error = re.compile('.*ERROR.*(units|cell).*(?!.*(time|boundary|coordinate)).*variable.*')
                cf_other_error = re.compile('.*ERROR.*(bound|Boundary|grid|coordinate|dimension).*')
                cf_abort = re.compile('.*suffix.*')

                regexlist =[(cf_global_error, "global"), (cf_variable_error, "variable"), (cf_other_error, "other"),
                            (cf_abort, "fatal")]

                for line in cf_out:
                    for regex, label in regexlist:
                        if regex.match(line.strip()):
                            make_qc_err_record(dfile, checkType, label, line, os.path.join(log_dir, file))
#                            print checkType, label, line, os.path.join(log_dir, file)

    else:

        if not os.path.exists(logfile):
            with open(logfile, 'w') as logger:
                logger.writelines(log_dir, '\n')
        else:
            with open(logfile, 'a') as logger:
                logger.writelines(log_dir, '\n')



                        #                    if cedacc_global_error.match(line.strip()):
#                        make_qc_err_record(dfile, checkType, "global", line, os.path.join(log_dir, file))
#                    if cedacc_variable_error.match(line.strip()):
#                        make_qc_err_record(dfile, checkType, "variable", line, os.path.join(log_dir, file))
#                    if cedacc_other_error.match(line.strip()):
#                        make_qc_err_record(dfile, checkType, "other", line, os.path.join(log_dir, file))
#                    if cedacc_exception.match(line.strip()):
 #                       make_qc_err_record(dfile, checkType, "fatal", line, os.path.join(log_dir, file))
 #                   if cedacc_abort.match(line.strip()):
 #                       make_qc_err_record(dfile, checkType, "fatal", line, os.path.join(log_dir, file))



def read_ceda_cc_files(dfile, qcfile):

    checkType = "CEDA-CC"
    temporal_range = qcfile.split("_")[-1].strip(".nc").split("_")[0]

    institute, model, experiment, frequency, realm, table, ensemble, version, variable, \
    ncfile = qcfile.split('/')[6:]
    log_dir = os.path.join(CEDACC_DIR, institute, model, experiment, table, version)
    if not os.path.exists(log_dir):
        with open('missing_dirs.log', 'a'):
            print log_dir
    else:
        file_list = os.listdir(log_dir)
        file_base = "_".join([variable, table, model, experiment, ensemble])
        file_pattern = re.compile(file_base + "_" + temporal_range + "__qclog_\d+\.txt")

        for file in file_list:
            if file_pattern.match(file):
                with open(os.path.join(log_dir, file), 'r') as fr:
                    ceda_cc_out = fr.readlines()

                # Identify where CEDA-CC picks up a QC error
                cedacc_global_error = re.compile('.*global.*FAILED::.*')
                cedacc_variable_error = re.compile('.*variable.*FAILED::.*')
                cedacc_other_error = re.compile('.*filename.*FAILED::.*')
                cedacc_exception = re.compile('.*Exception.*')
                cedacc_abort = re.compile('.*aborted.*')

                for line in ceda_cc_out:
                    if cedacc_global_error.match(line.strip()):
                        make_qc_err_record(dfile, checkType, "global", line, os.path.join(log_dir, file))
                    if cedacc_variable_error.match(line.strip()):
                        make_qc_err_record(dfile, checkType, "variable", line, os.path.join(log_dir, file))
                    if cedacc_other_error.match(line.strip()):
                        make_qc_err_record(dfile, checkType, "other", line, os.path.join(log_dir, file))
                    if cedacc_exception.match(line.strip()):
                        make_qc_err_record(dfile, checkType, "fatal", line, os.path.join(log_dir, file))
                    if cedacc_abort.match(line.strip()):
                        make_qc_err_record(dfile, checkType, "fatal", line, os.path.join(log_dir, file))


def make_qc_err_record(dfile, checkType, errorType, errorMessage, filepath):

    qc_err, _ = QCerror.objects.get_or_create(
                    file=dfile, check_type=checkType, error_type=errorType,
                    error_msg=errorMessage, report_filepath=filepath)


def read_qc_results(project):
    """
    Read in the CEDA-CC and CF results files and populate QCerror table
    CEDA-CC and the CF-checker are run on the filelists on lotus and 
    results are written to the group workspace.    
    """

    data_specs = DataSpecification.objects.filter(datarequesters__requested_by__contains=project)

    for dspec in data_specs:
        datasets = dspec.dataset_set.all()
        for dataset in datasets:

            datafiles = dataset.datafile_set.all()
            for d_file in datafiles:
                qcfile = str(d_file.archive_path)

                #read_ceda_cc_files(d_file, qcfile)

                read_cf_files(d_file, qcfile)


if __name__ == '__main__':
    # These constraints will in time be loaded in via csv for multiple projects.
    project = 'CMIP5'
    node = "172.16.150.171"
    expts = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
    distrib = False
    latest = True
    file = '/usr/local/cp4cds-app/project-specs/cp4cds-dmp_data_request.csv'
    # file = '/usr/local/cp4cds-app/project-specs/magic_data_request.csv'
    #    file = '/usr/local/cp4cds-app/project-specs/abc4cde_data_request.csv'
    # url = "https://172.16.150.171/esg-search/search?type=File&project=CMIP5&variable=tas&cmor_table=Amon&time_frequency=mon&model=HadGEM2-ES&experiment=historical&ensemble=r1i1p1&latest=True&distrib=False&format=application%%2Fsolr%%2Bjson&limit=10000"
    project = 'CP4CDS'
    read_qc_results(project)


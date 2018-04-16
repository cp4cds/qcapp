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

def run_ceda_cc(file, odir):
    """

    Runs CEDA-CC on the input file

    :param file: valid filepath to run CEDA-CC
    TODO: Check file exits
    TODO: Check CEDA-CC has run ok
    :return:
    """

    print("Running CEDA-CC")
    if not os.path.exists(file):
        with open("cedacc_error.log", 'a+') as fw:
            err_message = "{} : Does not exist \n".format(file)
            fw.writelines(err_message)
    else:
        institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = file.split('/')[6:]
        print(institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile)

        # Use facets to create directory for CEDA-CC output e.g. BASEDIR/model/experiment/table/<files>
        cedacc_odir = os.path.join(odir, institute, model, experiment, frequency, realm, version)
        if not os.path.exists(cedacc_odir):
            os.makedirs(cedacc_odir)

        # Run CEDA-CC
        cedacc_args = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', cedacc_odir, '--cae', '--blfmode', 'a']
        run_cedacc = c4.main(cedacc_args)
        print run_cedacc

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

def max_timeseries_qc_errors(ts):
    """
    Input is of the format of a dictionary of dictonary e.g.
    {'filename': {'global': 0, 'variable': 1, 'other', 1}}
    :param ts:
    :return:
    """

    max_errors = {'global': 0, 'variable': 0, 'other': 0}

    for key in ['global', 'variable', 'other']:
        errors = []
        for file, errs in ts.iteritems():
            errors.append(errs[key])
        max_errors[key] = max(errors)

    return max_errors

def get_total_qc_errors(qcfile):
    files = DataFile.objects.filter(ncfile=qcfile)
    # if files != 1:
    #    raise Exception("Length of files %s must not be greater than 1, length is %s: " % (qcfile, len(files)))

    file = files.first()
    qc_errors = file.qcerror_set.all()
    errors = {}
    errors['global'] = qc_errors.filter(error_type='global').count()
    errors['variable'] = qc_errors.filter(error_type='variable').exclude(error_msg__contains="ERROR (4)").count()
    errors['other'] = qc_errors.filter(error_type='other').exclude(error_msg__contains="ERROR (4)").count()

    return errors



def get_list_of_qc_files():

    for dataset in Dataset.objects.all():
        datafiles = dataset.datafile_set.all()
        for dfile in datafiles:
            qc_errors = dfile.qcerror_set.all()
            for error in qc_errors:
                path = error.file.archive_path
                file = error.file.ncfile



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


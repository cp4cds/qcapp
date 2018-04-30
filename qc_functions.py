import django

django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

import collections
import os
import timeit
import datetime
import time
import re
import glob
import commands
import requests, itertools
from subprocess import call
from netCDF4 import Dataset as ncDataset
from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version
from time_checks.run_file_timechecks import main as single_file_time_checks
from time_checks.run_multifile_timechecks import main as multi_file_time_checks
from esgf_dict import EsgfDict
from qc_settings import *
from is_latest import check_datafile_is_latest

def run_is_latest(variable, frequency, table):

    esgf_dict = EsgfDict([
        ("node", "esgf-index1.ceda.ac.uk"),
        ("project", "CMIP5"),
        ("frequency", frequency),
        ("table", table),
        ("variable", variable),
        ("distrib", "true"),
        ("latest", "true"),
    ])

    for experiment in ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']:
    # for experiment in ['historical']:
        esgf_dict['experiment'] = experiment
        datafiles = DataFile.objects.filter(variable=variable, dataset__frequency=frequency,
                                            dataset__cmor_table=table, dataset__experiment=experiment)
        check_datafile_is_latest(datafiles, esgf_dict)


def run_multifile_time_checker(datasets, var, table, expt):

    # ds = datasets.first()
    for ds in datasets:
        if ds.datafile_set.count() > 1:
            for d in ds.datafile_set.all():
                d.timeseries = True
                d.save()

            df = ds.datafile_set.first()
            ensemble = df.gws_path.split('/')[-4]
            version = "v" + os.readlink(df.gws_path).split('/')[-2]
            odir = os.path.join(QCLOGS, var, table, expt, ensemble, version)
            if not os.path.isdir(odir):
                os.makedirs(odir)

            f = os.path.basename(df.gws_path).strip('.nc').split('_')
            ofile = '_'.join(f[:-1]) + '__multifile_timecheck.log'
            logfile = os.path.join(odir, ofile)

            dir_of_files = os.path.dirname(df.gws_path)
            files = os.listdir(dir_of_files)
            filelist = []

            for f in files:
                if f.endswith('.nc'):
                    filelist.append(os.path.join(dir_of_files, f))

            multi_file_time_checks(filelist, logfile)


def file_time_checks(ifile, odir):

    try:
        d = ncDataset(ifile)
    except(IOError):
        d = None

    if isinstance(d, ncDataset):
        single_file_time_checks(ifile, odir)
    else:
        logfile = os.path.join(odir, ifile.replace('.nc', '__file_timecheck.log'))
        with open(logfile, 'w') as fw:
            fw.writelines(["Time checks of: {} \n".format(ifile)])
            fw.writelines(["T0.000::[FATAL]::Not a NetCDF file"])


def run_ceda_cc(file, odir):
    """

    Runs CEDA-CC on the input file

    :param file: valid filepath to run CEDA-CC
    :return:
    """
    if not os.path.exists(file):
        ofile = ncfile.replace('.nc', '__cedacc_error.log')
        with open(ofile, 'w+') as fw:
            err_message = "{} : Does not exist \n".format(file)
            fw.writelines(err_message)
    else:
        institute, model, experiment, frequency, realm, table, ensemble, variable, version, ncfile = file.split('/')[8:]
        ofile = ncfile.strip('.nc')
        now = datetime.datetime.now().strftime('%Y%m%d')
        ofile = "{}__qclog_{}.txt".format(ofile, now)
        if os.path.exists(ofile):
            print "{} exists not performing ceda-cc on {}".format(ofile, file)
        else:
            cedacc_args = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', odir, '--cae', '--blfmode', 'a',]
            run_cedacc = c4.main(cedacc_args)


def parse_ceda_cc(df_obj, odir):
    """
    Parses the CEDA-CC output on the input file.

    Finds any errors recorded by CEDA-CC and then makes a QCerror record for each found.

    :param dbobj:
    :param odir:
    :return:
    """

    checkType = "CEDA-CC"
    file_path = df_obj.gws_path
    temporal_range = file_path.split("_")[-1].strip(".nc").split("_")[0]
    institute, model, experiment, frequency, realm, table, ensemble, variable, latest, ncfile = file_path.split('/')[8:]
    file_base = "_".join([variable, table, model, experiment, ensemble, temporal_range])

    # Constructs a CEDA-CC regex based on variable_table_model_experiment_ensemble_temporal-range__qclog_{date}.txt
    ceda_cc_file_pattern = re.compile(file_base + "__qclog_\d+\.txt")

    # List files in the CEDA-CC logdir
    # log_dir = os.path.join(CEDACC_DIR,  institute, model, experiment, frequency, realm, version)
    log_dir_files = os.listdir(odir)

    for logfile in log_dir_files:

        # If the input file is in the logdir parse the output
        if ceda_cc_file_pattern.match(logfile):
            ceda_cc_file = os.path.join(odir, logfile)
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
                line = line.strip()
                if cedacc_global_error.match(line):
                    make_qc_err_record(df_obj, checkType, "global", line, ceda_cc_file)
                if cedacc_variable_error.match(line):
                    make_qc_err_record(df_obj, checkType, "variable", line, ceda_cc_file)
                if cedacc_other_error.match(line.strip()):
                    make_qc_err_record(df_obj, checkType, "other", line, ceda_cc_file)
                if cedacc_exception.match(line):
                    make_qc_err_record(df_obj, checkType, "fatal", line, ceda_cc_file)
                if cedacc_abort.match(line):
                    make_qc_err_record(df_obj, checkType, "fatal", line, ceda_cc_file)


def run_cf_checker(file, odir):
    """

    Run the CF-Checker on the input file from the shell by calling out using subprocess.call

    :param file: GWS NetCDF file
    """

    # Define output and error log files
    cf_out_file = os.path.join(odir, os.path.basename(file).replace(".nc", ".cf-log.txt"))
    cf_err_file = os.path.join(odir, os.path.basename(file).replace(".nc", ".cf-err.txt"))
    run_cmd = ["/usr/bin/cf-checker", "-a", AREATABLE, "-s", STDNAMETABLE, "-v", "auto", file]
    cf_out, cf_err = open(cf_out_file, "w"), open(cf_err_file, "w")
    call(run_cmd, stdout=cf_out, stderr=cf_err)
    cf_out.close(), cf_err.close()

    if os.path.getsize(cf_err_file) == 0:
        os.remove(cf_err_file)
    else:
        filen = file.replace('.nc', '.cf-err')
        filename = os.path.join(CF_FATAL_DIR, filen)
        touch_cmd = ["touch", filename]
        call(touch_cmd)


def parse_cf_checker(df, file, log_dir):
    """

    Parses the CF-Checker output for the input file

    Finds any errors recorded by the CF-Checker and then makes a QCerror record for each found.

    :param file: Archive file
    TODO: check it is a valid file?

    :return:
    """
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

    checkType = "CF"

    temporal_range = file.split("_")[-1].strip(".nc")
    institute, model, experiment, frequency, realm, table, ensemble, variable, latest, ncfile = file.split('/')[8:]
    file_base = "_".join([variable, table, model, experiment, ensemble, temporal_range])

    # Constructs a CF file regex based on variable_table_model_experiment_ensemble_temporal-range.cf-log.txt
    cf_file_pattern = re.compile(file_base + ".cf-log.txt")

    # List files in the CF logdir
    # log_dir = os.path.join(CF_DIR, institute, model, experiment, frequency, realm, version)
    log_dir_files = os.listdir(log_dir)

    cf_out = None
    for logfile in log_dir_files:

        # If the input file is in the logdir parse the output
        if cf_file_pattern.match(logfile):
            with open(os.path.join(log_dir, logfile), 'r') as fr:
                cf_out = fr.readlines()

    if not cf_out:
        make_qc_err_record(df, checkType, 'FATAL', 'NO CF-LOG FILE', os.path.join(log_dir, logfile))
    else:

        # Identify where CF picks up a QC error
        for line in cf_out:
            for regex, label in regexlist:
                if regex.match(line.strip()):
                    make_qc_err_record(df, checkType, label, line, os.path.join(log_dir, logfile))


def make_qc_err_record(dfile, checkType, errorType, errorMessage, filepath):

    qc_err, _ = QCerror.objects.get_or_create(
        file=dfile, check_type=checkType, error_type=errorType,
        error_msg=errorMessage, report_filepath=filepath)

    # TODO: Must add in a test for a non-zero .cf-err.txt and record perhaps retry or read in only here





# def max_timeseries_qc_errors(ts):
#     """
#     Input is of the format of a dictionary of dictonary e.g.
#     {'filename': {'global': 0, 'variable': 1, 'other', 1}}
#     :param ts:
#     :return:
#     """
#
#     max_errors = {'global': 0, 'variable': 0, 'other': 0}
#
#     for key in ['global', 'variable', 'other']:
#         errors = []
#         for file, errs in ts.iteritems():
#             errors.append(errs[key])
#         max_errors[key] = max(errors)
#
#     return max_errors
#
#
# def get_total_qc_errors(qcfile):
#     files = DataFile.objects.filter(ncfile=qcfile)
#     # if files != 1:
#     #    raise Exception("Length of files %s must not be greater than 1, length is %s: " % (qcfile, len(files)))
#
#     file = files.first()
#     qc_errors = file.qcerror_set.all()
#     errors = {}
#     errors['global'] = qc_errors.filter(error_type='global').count()
#     errors['variable'] = qc_errors.filter(error_type='variable').exclude(error_msg__contains="ERROR (4)").count()
#     errors['other'] = qc_errors.filter(error_type='other').exclude(error_msg__contains="ERROR (4)").count()
#
#     return errors
#
#
#
# def get_list_of_qc_files():
#
#     for dataset in Dataset.objects.all():
#         datafiles = dataset.datafile_set.all()
#         for dfile in datafiles:
#             qc_errors = dfile.qcerror_set.all()
#             for error in qc_errors:
#                 path = error.file.archive_path
#                 file = error.file.ncfile


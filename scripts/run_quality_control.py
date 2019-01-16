#!/usr/bin/env python
"""
A driver routine that will run the quality control related routines specified.
"""

from setup_django import *
import sys
import re
from settings import *
from utils import *
from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version
from time_checks.run_file_timechecks import main as single_file_time_checks
from time_checks.run_multifile_timechecks import main as multi_file_time_checks
from is_latest import check_datafile_is_latest
import subprocess
# from utils import *
# from qc_functions import *
# from is_latest import *
#



def make_qc_err_record(dfile, checkType, errorType, errorMessage, filepath, errorLevel=None):

    qc_err, _ = QCerror.objects.get_or_create(file=dfile,
                                              check_type=checkType,
                                              error_type=errorType,
                                              error_msg=errorMessage,
                                              report_filepath=filepath,
                                              error_level=errorLevel
                                             )


def make_ds_qcerr_record(dset, checkType, errorType, errorMessage, filepath, errorLevel=None):
    qc_err, _ = QCerror.objects.get_or_create(set=dset,
                                              check_type=checkType,
                                              error_type=errorType,
                                              error_msg=errorMessage,
                                              report_filepath=filepath,
                                              error_level=errorLevel
                                              )


def run_cfchecker(file, qcdir):
    
    cf_log_exists, cf_logfile = check_log_exists(file, qcdir, '.cf-log.txt')
    if not cf_log_exists:
        cf_out_file = os.path.join(qcdir, os.path.basename(file).replace(".nc", ".cf-log.txt"))
        cf_err_file = os.path.join(qcdir, os.path.basename(file).replace(".nc", ".cf-err.txt"))
        run_cmd = ["/usr/bin/cf-checker", "-a", AREATABLE, "-s", STDNAMETABLE, "-v", "auto", file]
        cf_out, cf_err = open(cf_out_file, "w"), open(cf_err_file, "w")
        subprocess.call(run_cmd, stdout=cf_out, stderr=cf_err)
        cf_out.close(), cf_err.close()

        if os.path.getsize(cf_err_file) == 0:
            os.remove(cf_err_file)


def parse_cf_checker(df_obj, qcdir):
    """

    Parses the CF-Checker output for the input file

    Finds any errors recorded by the CF-Checker and then makes a QCerror record for each found.

    :param file: Archive file
    TODO: check it is a valid file?

    :return:
    """

    # Dictionary mapping the CF regex with type of error
    cf_regex_errors = [
                        (re.compile('.*ERROR.*(global|Global|Convention).*'), "global"),
                        (re.compile('.*ERROR.*(units|cell).*(?!.*(time|boundary|coordinate|co-ordinate)).*'), "variable"),
                        (re.compile('.*ERROR.*(bound|Boundary|grid|coordinate|co-ordinate|dimension).*'), "other"),
                        (re.compile('.*suffix.*'), "fatal"),
                        (re.compile('.*COULD NOT OPEN FILE.*'), "fatal")
                      ]

    checkType = "CF"
    cf_log_exists, cf_logfile = check_log_exists(df_obj.gws_path, qcdir, '.cf-log.txt')
    cf_logfile_path = os.path.join(qcdir, cf_logfile)
    if cf_log_exists:

        with open(cf_logfile_path, 'r') as fr:
            cf_out = fr.readlines()

        for line in cf_out:
            line = line.strip()
            for regex, errType in cf_regex_errors:
                if regex.search(line):
                    make_qc_err_record(df_obj, checkType, errType, line, cf_logfile_path)

    else:
        make_qc_err_record(df_obj, checkType, 'FATAL', 'NO CF-LOG FILE', cf_logfile_path)


def run_ceda_cc(file, qcdir):
    
    cc_log_exists, cc_logfile = check_log_exists(file, qcdir, '__qclog_')
    if not cc_log_exists:
        # print("PERFORMING CEDA-CC")
        cedacc_args = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', qcdir, '--cae', '--blfmode', 'a',]
        run_cedacc = c4.main(cedacc_args)
        


def parse_ceda_cc(df_obj, qcdir):
    """
    Parses the CEDA-CC output on the input file.

    Finds any errors recorded by CEDA-CC and then makes a QCerror record for each found.

    :param dbobj:
    :param qcdir:
    :return:
    """

    checkType = "CEDA-CC"
    cc_log_exists, cc_logfile = check_log_exists(df_obj.gws_path, qcdir, '__qclog_')

    if cc_log_exists:
        ceda_cc_file = os.path.join(qcdir, cc_logfile)
        with open(ceda_cc_file, 'r') as fr:
            ceda_cc_out = fr.readlines()

        ccc_regex_errors = [
                            (re.compile('.*global.*FAILED::.*'), "global"),
                            (re.compile('.*variable.*FAILED::.*'), "variable"),
                            (re.compile('.*filename.*FAILED::.*'), "other"),
                            (re.compile('.*FAILED:: Exception has occured.*'), "fatal"),
                            (re.compile('.*(aborted|ABORTED).*'), "fatal"),
                           ]

        # For CEDA-CC ouput search for errors and if found make a QCerror record
        for line in ceda_cc_out:
            line = line.strip()
            if "Variable [sos] has incorrect attributes" in line:
                continue
            for regex, errType in ccc_regex_errors:
                if regex.search(line):
                    make_qc_err_record(df_obj, checkType, errType, line, ceda_cc_file)




def run_file_time_checks(file, qcdir):
    tc_log_exists, tc_log_file = check_log_exists(file, qcdir, '__file_timecheck.log')
    if not tc_log_exists:
        single_file_time_checks(file, qcdir)


def run_time_series_check(files, qcdir):

    _var, _table, _model, _exp, _ens, = os.path.basename(files[0]).split('_')[:-1]
    file = '_'.join([_var, _table, _model, _exp, _ens])
    tsc_log_file = os.path.join(qcdir, file+'__multifile_timecheck.log')

    if not os.path.exists(tsc_log_file):
        multi_file_time_checks(files, tsc_log_file)

# TODO CONVERT + ADD error LEVELS....

# def parse_singlefile_timechecks(df_obj, log_dir):
#     """
#     Parses the CEDA-CC output on the input file.
#
#     Finds any errors recorded by CEDA-CC and then makes a QCerror record for each found.
#
#     :param df_obj: Model datafile object
#     :param log_dir:
#     :return:
#     """
#
#     checkType = "TEMPORAL"
#     file_path = df_obj.gws_path
#     temporal_range = file_path.split("_")[-1].strip(".nc").split("_")[0]
#     institute, model, experiment, frequency, realm, table, ensemble, variable, latest, ncfile = file_path.split('/')[8:]
#     file_timecheck_filename = "_".join(
#         [variable, table, model, experiment, ensemble, temporal_range]) + "__file_timecheck.log"
#     file_timecheck_logfile = os.path.join(log_dir, file_timecheck_filename)
#
#     file_temporal_fatal = re.compile('.*FATAL.*|.*File does not end with.*')
#     file_temporal_fail = re.compile('.*FAIL.*')
#
#     file_regexlist = [(file_temporal_fatal, "fatal"),
#                       (file_temporal_fail, "fail")]
#
#     if os.path.exists(file_timecheck_logfile):
#         with open(file_timecheck_logfile, 'r') as fr:
#             file_timecheck_data = fr.readlines()
#
#             for line in file_timecheck_data:
#                 for regex, label in file_regexlist:
#                     if regex.match(line.strip()):
#                         make_qc_err_record(df_obj, checkType, label, line, file_timecheck_logfile)
#     else:
#         make_qc_err_record(df_obj, checkType, "FATAL", "Timecheck log file does not exist", file_timecheck_logfile)
#
#
# def parse_multifile_timechecks(ds_obj):
#
#     """
#     # def parse_multifile_timechecks(ds_obj, log_dir):
#
#     Parses the CEDA-CC output on the input file.
#
#     Finds any errors recorded by CEDA-CC and then makes a QCerror record for each found.
#
#     :param df_obj: Model datafile object
#     :param log_dir:
#     :return:
#     """
#     basedir = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/QC_LOGS/"
#     checkType = "TIME-SERIES"
#     if ds_obj.is_timeseries == True:
#         try:
#             path = ds_obj.datafile_set.first().gws_path
#         except:
#             make_ds_qcerr_record(ds_obj, checkType, "FATAL", "No datafiles found",
#                                  "", errorLevel='FATAL')
#             return
#         _ins, _mod, _exp, _freq, _realm, _table, _ens, _var, _v, _ncfile = path.split('/')[8:]
#         qcdir = os.path.join(basedir, _var, _table, _exp, _ens, ds_obj.version)
#         ofile = '_'.join([_var, _table, _mod, _exp, _ens]) + '__multifile_timecheck.log'
#         multifile_timecheck_logfile = os.path.join(qcdir, ofile)
#         multifile_temporal_fatal = re.compile('.*Error.*')
#         multifile_temporal_fail = re.compile('.*FAIL.*')
#
#
#         multifile_regexlist = [(multifile_temporal_fatal, "fatal"),
#                                (multifile_temporal_fail, "fail")]
#
#         if os.path.exists(multifile_timecheck_logfile):
#             with open(multifile_timecheck_logfile, 'r') as fr:
#                 multifile_timecheck_data = fr.readlines()
#
#                 for line in multifile_timecheck_data:
#                     for regex, label in multifile_regexlist:
#                         if regex.match(line.strip()):
#                             make_ds_qcerr_record(ds_obj, checkType, label, line, multifile_timecheck_logfile)
#         else:
#             make_ds_qcerr_record(ds_obj, checkType, "FATAL", "Multifile timecheck log file does not exist",
#                                multifile_timecheck_logfile, errorLevel='FATAL')




def run_check_is_latest(datafile):

    check_datafile_is_latest(datafile)



def run_qc(variable, frequency, table):

    for experiment in ALLEXPTS:

        datasets = Dataset.objects.filter(variable=variable, frequency=frequency, cmor_table=table, experiment=experiment)
        
        for ds in datasets:
            datafiles = ds.datafile_set.all()

            if len(datafiles) > 1:
                qc_logdir = get_and_make_logdir(datafiles[0])
                run_time_series_check(datafiles, qc_logdir)

            for datafile in datafiles:
                qc_logdir = get_and_make_logdir(datafile)
                run_ceda_cc(datafile.gws_path, qc_logdir)
                parse_ceda_cc(datafile, qc_logdir)
                run_cf_checker(datafile.gws_path, qc_logdir)
                parse_cf_checker(datafile, qc_logdir)
                run_file_time_checks(datafile.gws_path, qc_logdir)
                run_check_is_latest(datafile)



if __name__ == "__main__":

    variable = sys.argv[1]
    frequency = sys.argv[2]
    table = sys.argv[3]

    run_qc(variable, frequency, table)
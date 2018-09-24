
import os
import re
import glob
from qc_settings import *
from utils import *
from esgf_dict import EsgfDict


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


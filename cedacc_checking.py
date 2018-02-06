

import os
import re
import glob
from ceda_cc import c4
from qc_settings import *
from utils import *
from esgf_dict import EsgfDict

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



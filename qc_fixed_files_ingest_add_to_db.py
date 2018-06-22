

import django
django.setup()
import os
import re
import fnmatch
import json
import requests
import commands
import datetime
import argparse
import filecmp
import uuid
import shutil

from django.db.models import Q
from netCDF4 import Dataset as ncDataset ## Conflict with local naming convention Dataset is a db model!!
from subprocess import call
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from sys import argv
from qcapp.models import *
from utils import *
from qc_settings import *
from qc_functions import *
requests.packages.urllib3.disable_warnings()

ARCHIVE_ROOT = "/badc/cmip5/data/"
WEBROOT = "http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/"
ARCHIVE_BASEDIR = "/badc/cmip5/data/cmip5/output1/"
GWS_BASEDIR = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/"
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"
NEW_DATA_DIR = "/group_workspaces/jasmin2/cp4cds1/data/corrected/v20180618"
QCDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/QC_LOGS"

def _get_facets_from_gws_name(ifile):

    institute, model, experiment, frequency, realm, table, ensemble, variable, latest, ncfile = ifile.split('/')[8:]
    return institute, model, experiment, frequency, realm, table, ensemble, variable



def _run_ceda_cc(file, odir):
    """

    Runs CEDA-CC on the input file

    :param file: valid new filepath to run CEDA-CC
    :return:
    """
    if not os.path.isdir(odir):
        os.makedirs(odir)


    if not os.path.exists(file):
        ofile = os.path.basename(file).replace('.nc', '__cedacc_error.log')
        with open(os.path.join(odir, ofile), 'w+') as fw:
            err_message = "{} : Does not exist \n".format(file)
            fw.writelines(err_message)
    else:
        # os.path.join(odir, os.path.basename(file).replace(".nc", ".cf-log.txt")
        # institute, model, experiment, frequency, realm, table, ensemble, variable, version, ncfile = file.split('/')[8:]
        # ofile = ncfile.strip('.nc')
        # now = datetime.datetime.now().strftime('%Y%m%d')
        # ofile = "{}__qclog_{}.txt".format(ofile, now)
        # if os.path.exists(ofile):
        #     print "{} exists not performing ceda-cc on {}".format(ofile, file)
        # else:
        cedacc_args = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', odir, '--cae', '--blfmode', 'a',]
        run_cedacc = c4.main(cedacc_args)


def _run_cf_checker(file, odir):
    """

    Run the CF-Checker on the input file from the shell by calling out using subprocess.call

    :param file: GWS NetCDF file
    """

    print "cf checking {}".format(file)

    if not os.path.isdir(odir):
        os.makedirs(odir)
    # Define output and error log files
    cf_out_file = os.path.join(odir, os.path.basename(file).replace(".nc", ".cf-log.txt"))
    cf_err_file = os.path.join(odir, os.path.basename(file).replace(".nc", ".cf-err.txt"))
    run_cmd = ["/usr/bin/cf-checker", "-a", AREATABLE, "-s", STDNAMETABLE, "-v", "auto", file]
    cf_out, cf_err = open(cf_out_file, "w"), open(cf_err_file, "w")
    call(run_cmd, stdout=cf_out, stderr=cf_err)
    cf_out.close(), cf_err.close()

    if os.path.getsize(cf_err_file) == 0:
        os.remove(cf_err_file)
    # else:
        # filen = os.path.basename(file).replace('.nc', '.cf-err')
        # filename = os.path.join(odir, filen)
        # touch_cmd = ["touch", filename]
        # call(touch_cmd)



def _read_cf_checker(file, log_dir):
    """

    Parses the CF-Checker output for the input file

    Finds any errors recorded by the CF-Checker and then makes a QCerror record for each found.

    :param file: ncfile name
    :param log_dir: directory with log file
    :return: [BOOL] PASS FAIL
    """


    # CF regex expressions for errors
    qc_pass = True
    cf_errors = [
                'ERROR (7.3): Invalid unit mintues) in cell_methods comment',
                "ERROR (5): co-ordinate variable 'time' not monotonic",
                "ERROR (5): co-ordinate variable 'lon' not monotonic",
                "ERROR (5): co-ordinate variable 'plev' not monotonic",
                "ERROR (5): co-ordinate variable 'lat' not monotonic",
                'ERROR (3.1): Invalid units:  psu', u'ERROR (7.3) Invalid syntax for cell_methods attribute',
                'COULD NOT OPEN FILE, PLEASE CHECK THAT NETCDF IS FORMATTED CORRECTLY.',
                ]

    cf_log_file = os.path.join(log_dir, file.replace('.nc', '.cf-log.txt'))
    if not os.path.exists(cf_log_file):
        print("BAD NO LOG FILE {}".format(cf_log_file))
        return False

    else:
        with open(cf_log_file) as r:
            lines = r.readlines()

        for line in lines:
            line = line.strip()
            for e in cf_errors:
                if e in line:
                    return False

    return qc_pass



def _read_cedacc(file, log_dir):
    """

    Parses the CF-Checker output for the input file

    Finds any errors recorded by the CF-Checker and then makes a QCerror record for each found.

    :param file: ncfile name
    :param log_dir: directory with log file
    :return: [BOOL] PASS FAIL
    """


    # CF regex expressions for errors
    qc_pass = True
    cc_errors = [
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [tos] has incorrect attributes: standard_name="surface_temperature" [correct: "sea_surface_temperature"]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [uas] has incorrect attributes: long_name="Eastward Near-Surface Wind" [correct: "Eastward Near-Surface Wind Speed"]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [od550aer] has incorrect attributes: long_name="Ambient Aerosol Opitical Thickness at 550 nm" [correct: "Ambient Aerosol Optical Thickness at 550 nm"]',
                "C4.002.004: [variable_ncattribute_present]: FAILED:: Required variable attributes missing: ['standard_name']",
                'C4.003.xxx: ABORTED:: Errors too severe to complete further checks in this module',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [vas] has incorrect attributes: long_name="Northward Near-Surface Wind Speed" [correct: "Northward Near-Surface Wind"]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [evspsbl]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [rsdt]',
                'C4.100.001: [exception]: FAILED:: Exception has occured',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [ps]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [prsn] has incorrect attributes: long_name="Solid Precipitation" [correct: "Snowfall Flux"]',
                "C4.002.007: [filename_filemetadata_consistency]: FAILED:: File name segments do not match corresponding global attributes: [(2, 'model_id')]",
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [tas]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [uas] has incorrect attributes: long_name="Eastward Near-Surface Wind Speed" [correct: "Eastward Near-Surface Wind"]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [psl]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [mrsos] has incorrect attributes: long_name="Moisture in Upper 0.1 m of Soil Column" [correct: "Moisture in Upper Portion of Soil Column"]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [sos] has incorrect attributes: units="1" [correct: "psu"]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [tasmax]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [vas] has incorrect attributes: long_name="Northward Near-Surface Wind" [correct: "Northward Near-Surface Wind Speed"]',
                'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [pr]',
            ]


    ceda_cc_file_pattern = re.compile(file.strip('.nc') + "__qclog_\d+\.txt")
    # List files in the CEDA-CC logdir
    log_dir_files = os.listdir(log_dir)
    for logfile in log_dir_files:
        # If the input file is in the logdir parse the output
        if ceda_cc_file_pattern.match(logfile):
            ceda_cc_file = os.path.join(log_dir, logfile)
            if not os.path.exists(ceda_cc_file):
                print("BAD NO LOG FILE {}".format(ceda_cc_file))
                return False
            else:
                with open(ceda_cc_file, 'r') as fr:
                    ceda_cc_out = fr.readlines()

    for line in ceda_cc_out:
        line = line.strip()
        for e in cc_errors:
            if e in line:
                return False

    return qc_pass



def main(ncfile):

    df = DataFile.objects.filter(ncfile=ncfile).first()
    institute, model, experiment, frequency, realm, table, ensemble, variable = _get_facets_from_gws_name(df.gws_path)
    new_version = 'v20180618'
    qc_log_dir = os.path.join(QCDIR, variable, table, experiment, ensemble, new_version)
    _run_cf_checker(os.path.join(NEW_DATA_DIR, ncfile), qc_log_dir)
    cf_pass = _read_cf_checker(ncfile, qc_log_dir)
    _run_ceda_cc(os.path.join(NEW_DATA_DIR, ncfile), qc_log_dir)
    cc_pass = _read_cedacc(ncfile, qc_log_dir)

    if not cf_pass or not cc_pass:
        with open('/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/files_not_fixed_correctly.log', 'a+') as fw:
            fw.writelines(["{}\n".format(os.path.join(NEW_DATA_DIR, ncfile))])
    else:
        with open('/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/corrected_files.log', 'a+') as f:
            f.writelines(["{}\n".format(os.path.join(NEW_DATA_DIR, ncfile))])

if __name__ == "__main__":
    """
    This script will 
    1. Redo a CF and CEDA-CC check on the file
    2. Check the output contains no errors
    3. Will then copy this file back into the gws-archive with a new version
    4. Will add this to CP4CDS database
    5. Will append to a list of new files to be published to the CP4CDS index node
    files = os.listdir(NEW_DATA_DIR)
    for ncfile in files[:1]:
    print ncfile
    """

    ncfile = argv[1]
    main(os.path.basename(ncfile))


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
from netCDF4 import Dataset as ncDataset

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
NEW_DATA_DIR = "/group_workspaces/jasmin2/cp4cds1/data/corrected/v20180531"
# for datafiles in DataFile.objects.all():

parser = argparse.ArgumentParser()
parser.add_argument('--set_cf_error_levels',action='store_true', help='Set the CF-error level')
parser.add_argument('--set_cedacc_error_levels',action='store_true', help='Set the CEDA CC error level')
parser.add_argument('--cf_only_fixer',action='store_true', help='Check CF-errors are unique')


        # ncatted -a units,sos,o,c,'1e-3'

def _get_cell_methods_contents(ifile):

    variable = os.path.basename(ifile).split('_')[0]
    ncds = ncDataset(ifile)
    v = ncds.variables[variable]
    return getattr(v, 'cell_methods')

def _ncatted_common_updates(ofile, error_info):

    mod_date = datetime.datetime.now().strftime('%Y-%m-%d')

    cp4cds_statement = "As part of the Climate Projections for the Copernicus Data Store (CP4CDS) CMIP5 quality assurance " \
                       "testing the following error(s) were corrected by the CP4CDS team:: {}. " \
                       "The tracking id was also updated. " \
                       "For further details contact ruth.petrie@ceda.ac.uk".format(error_info)

    run_ncatted('tracking_id', 'global', 'o', 'c', str(uuid.uuid4()), ofile)
    run_ncatted('cp4cds_update_info', 'global', 'c', 'c', cp4cds_statement, ofile, noHistory=True)
    run_ncatted('history', 'global', 'a', 'c',
                "Updates made on {} were made as part of CP4CDS project see cp4cds_update_info".format(mod_date), ofile)


def fix_cf_73b(ifile):
    """
    By quick inspection it is determined that the error here is

        time: minimum (interval: 3 hours) within days time: mean over days
    it should be written
        time: minimum within days (interval: 3 hours) time: mean over days

    :param ifile:
    :return:
    """

    ofile = os.path.join(NEW_DATA_DIR, os.path.basename(ifile))
    cellMethod = _get_cell_methods_contents(ifile)

    interval_before_within = re.compile("\(interval.*?within")
    if re.search(interval_before_within, cellMethod):
        # wrong ordering discovered and must reorder...
        _parts = cellMethod.split()
        _parts[2:4], _parts[4:7] = _parts[5:7], _parts[2:5]
        corrected_cellMethod =' '.join(_parts)

        error_info = "ERROR (7.3) Invalid syntax for cell_methods attribute - information given in wrong order"
        run_ncatted('cell_methods', variable, 'o', 'c', corrected_cellMethod, ifile, newfile=ofile)
        _ncatted_common_updates(ofile, error_info)



def fix_cf_73a(ifile):

    ofile = os.path.join(NEW_DATA_DIR, os.path.basename(ifile))
    cellMethod = _get_cell_methods_contents(ifile)
    corrected_cellMethod = cellMethod.replace('mintues', 'minutes')

    error_info = "Invalid unit mintues) in cell_methods comment - a cell_methods typo"

    run_ncatted('cell_methods', variable, 'o', 'c', corrected_cellMethod, ifile, newfile=ofile)
    _ncatted_common_updates(ofile, error_info)


def fix_cf_31(ifile):
    print ifile


def run_ncatted(att_nm, var_nm, mode, att_type, att_val, file, newfile=None, noHistory=False):

    if noHistory: history = 'h'
    else: history = ''
    if not newfile:
        run_cmd = ["ncatted", "-{}a".format(history), "{},{},{},{},{}".format(att_nm, var_nm, mode, att_type, att_val), file]
        call(run_cmd)
    else:
        run_cmd = ["ncatted", "-{}a".format(history), "{},{},{},{},{}".format(att_nm, var_nm, mode, att_type, att_val), file, newfile]
        call(run_cmd)


def cf_fix_wrapper(filepath, error_message):


    if error_message == "ERROR (7.3): Invalid unit mintues) in cell_methods comment":
        fix_cf_73a(filepath)
    if error_message == "ERROR (3.1): Invalid units:  psu":
        print error_message
        fix_cf_31(filepath)
    if error_message == "ERROR (7.3) Invalid syntax for cell_methods attribute":
        fix_cf_73b(filepath)




def fix_cf_errors():


    cf_errs = QCerror.objects.filter(check_type='CF', file__duplicate_of=None)
    e_msgs = ["ERROR (7.3): Invalid unit mintues) in cell_methods comment",
              "ERROR (3.1): Invalid units:  psu",
              "ERROR (7.3) Invalid syntax for cell_methods attribute",
              ]

    for error_message in e_msgs[1:2]:
        errs = cf_errs.filter(error_msg=error_message)
        for e in errs[:1]:
            fpath = e.file.gws_path
            same_file_errs = e.file.qcerror_set.all()
            multiple_errs = same_file_errs.filter(check_type='CF').exclude(check_type='CF',error_msg=error_message).filter(check_type='CEDA-CC')
            if len(multiple_errs) == 0:
                cf_fix_wrapper(fpath, error_message)
            else:
                continue



def set_ceda_cc_error_level():
    cc_errs = QCerror.objects.filter(check_type='CEDA-CC')

    cce = cc_errs.values_list('error_msg', flat=True).distinct()
    get_values_list = False
    if get_values_list:
        cce = cc_errs.values_list('error_msg', flat=True).distinct()
        for e in cce:
            print "{} :: {}".format(e, cc_errs.filter(error_msg=e).count())

    cc_error_msg_level_dict = {}
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [tos] has incorrect attributes: standard_name="surface_temperature" [correct: "sea_surface_temperature"]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [uas] has incorrect attributes: long_name="Eastward Near-Surface Wind" [correct: "Eastward Near-Surface Wind Speed"]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [od550aer] has incorrect attributes: long_name="Ambient Aerosol Opitical Thickness at 550 nm" [correct: "Ambient Aerosol Optical Thickness at 550 nm"]'] = "FAIL"
    cc_error_msg_level_dict["C4.002.004: [variable_ncattribute_present]: FAILED:: Required variable attributes missing: ['standard_name']"] = "FAIL"
    cc_error_msg_level_dict['C4.003.xxx: ABORTED:: Errors too severe to complete further checks in this module'] = "FATAL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [vas] has incorrect attributes: long_name="Northward Near-Surface Wind Speed" [correct: "Northward Near-Surface Wind"]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [evspsbl]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [rsdt]'] = "FAIL"
    cc_error_msg_level_dict['C4.100.001: [exception]: FAILED:: Exception has occured'] = "FATAL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [ps]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [prsn] has incorrect attributes: long_name="Solid Precipitation" [correct: "Snowfall Flux"]'] = "FAIL"
    cc_error_msg_level_dict["C4.002.007: [filename_filemetadata_consistency]: FAILED:: File name segments do not match corresponding global attributes: [(2, 'model_id')]"] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [tas]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [uas] has incorrect attributes: long_name="Eastward Near-Surface Wind Speed" [correct: "Eastward Near-Surface Wind"]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [psl]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [sos] has incorrect attributes: units="1.e-3" [correct: "psu"]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [mrsos] has incorrect attributes: long_name="Moisture in Upper 0.1 m of Soil Column" [correct: "Moisture in Upper Portion of Soil Column"]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [sos] has incorrect attributes: units="1" [correct: "psu"]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [tasmax]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [vas] has incorrect attributes: long_name="Northward Near-Surface Wind" [correct: "Northward Near-Surface Wind Speed"]'] = "FAIL"
    cc_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [pr]'] = "FAIL"

    for err in cc_errs:
        if err.error_msg in cc_error_msg_level_dict:
            err.error_level = cc_error_msg_level_dict[err.error_msg]
        else:
            err.error_level = "FATAL :: UNKNOWN ERROR"
        err.save()

def set_cf_error_level():

    logdir_base = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/QC_LOGS/"
    cf_errs = QCerror.objects.filter(check_type='CF')

    cfe = cf_errs.values_list('error_msg', flat=True).distinct()

    for e in cfe:
        print "{} :: {}".format(e, cf_errs.filter(error_msg=e).count())

    # DONE - ASSIGNED
    cf_error_msg_level_dict = {}
    cf_error_msg_level_dict["ERROR (7.3): Invalid unit mintues) in cell_methods comment"] = "FAIL"
    cf_error_msg_level_dict["ERROR (5): co-ordinate variable 'time' not monotonic"] = "FATAL :: NOT FIXABLE"
    cf_error_msg_level_dict["ERROR (5): co-ordinate variable 'lon' not monotonic"] = "FATAL :: NOT FIXABLE"
    cf_error_msg_level_dict["ERROR (5): co-ordinate variable 'plev' not monotonic"] = "FATAL :: NOT FIXABLE"
    cf_error_msg_level_dict["ERROR (4): Axis attribute is not allowed for auxillary coordinate variables."] = "INFO :: IGNORING"
    cf_error_msg_level_dict["ERROR (5): co-ordinate variable 'lat' not monotonic"] = "FATAL :: NOT FIXABLE"
    cf_error_msg_level_dict["ERROR (3.1): Invalid units:  psu"] = "FAIL"
    cf_error_msg_level_dict["ERROR (7.3) Invalid syntax for cell_methods attribute"] = "FAIL"


    for err in cf_errs:
        if err.error_msg in cf_error_msg_level_dict:
           err.error_level = cf_error_msg_level_dict[err.error_msg]
        else:
            err.error_level = "FATAL :: UNKNOWN ERROR"

if __name__ == "__main__":
    args = parser.parse_args()

    if args.cf_only_fixer:
        fix_cf_errors()

    if args.set_cf_error_levels:
        set_cf_error_level()

    if args.set_cedacc_error_levels:
        set_ceda_cc_error_level()

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

class NCatted(object):

    def _run_ncatted(self, att_nm, var_nm, mode, att_type, att_val, file, newfile=None, noHistory=False):
        """
        Python wrapper to ncatted.

        :param att_nm: attribute name, e.g. history
        :param var_nm: variable name, e.g. global
        :param mode: change mode, eg, a: append, c: create
        :param att_type: attribute type, e.g. c: character, f: float
        :param att_val: attribute value, e.g. 1e-03
        :param file: file to modify default is in place modification
        :param newfile: specificy new output file
        :param noHistory: do not append change to the history attribute
        :return:
        """
        if noHistory:
            history = 'h'
        else:
            history = ''
        if not newfile:
            run_cmd = ["ncatted", "-{}a".format(history),
                       "{},{},{},{},{}".format(att_nm, var_nm, mode, att_type, att_val), file]
            call(run_cmd)
        else:
            run_cmd = ["ncatted", "-{}a".format(history),
                       "{},{},{},{},{}".format(att_nm, var_nm, mode, att_type, att_val), file, newfile]
            call(run_cmd)


class QCerror_fixer(object):

    """
    QCerror fixer a class of methods used to make QC changes to files.
    """

    ncatt = NCatted()

    qc_mapping = {
    "ERROR (7.3): Invalid unit mintues) in cell_methods comment": ('fix_cf_73a', ['filepath', 'error_message']),
    "ERROR (3.1): Invalid units:  psu": ('fix_cf_31',  ['filepath', 'error_message']),
    "ERROR (7.3) Invalid syntax for cell_methods attribute": ('fix_cf_73b', ['filepath', 'error_message']),
    'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: '
        'Variable [tos] has incorrect attributes: standard_name="surface_temperature" '
        '[correct: "sea_surface_temperature"]': ('fix_c4_002_005_tos',  ['filepath', 'error_message']),
    "C4.002.004: [variable_ncattribute_present]: FAILED:: "
        "Required variable attributes missing: ['standard_name']": ('fix_c4_002_005_tsice',  ['filepath', 'error_message']),
    "C4.002.007: [filename_filemetadata_consistency]: FAILED:: "
        "File name segments do not match corresponding "
        "global attributes: [(2, 'model_id')]": ('fix_c4_002_007_model_id',  ['filepath', 'error_message']),
    'missing_value must be present if _FillValue': ('fix_c4_002_005_missing_value',  ['filepath', 'error_message']),
    'long_name': ('fix_c4_002_005_long_name',  ['filepath', 'error_message']),
    }

    def _get_cell_methods_contents(self, ifile):
        """
        From the input file get the cell methods content

        :param ifile:
        :return: [string] cell method content
        """
        variable = os.path.basename(ifile).split('_')[0]
        ncds = ncDataset(ifile)
        v = ncds.variables[variable]
        return getattr(v, 'cell_methods')

    def _ncatted_common_updates(self, file, errors):
        """
        A set of common updates used when all other QC modifications are completed.

        :param file: ncfile name
        :param errors: list
        :return:
        """

        mod_date = datetime.datetime.now().strftime('%Y-%m-%d')

        cp4cds_statement = "As part of the Climate Projections for the Copernicus Data Store (CP4CDS) CMIP5 quality assurance " \
                           "testing the following error(s) were corrected by the CP4CDS team:: \n {} \n " \
                           "The tracking id was also updated; the original is stored under source_trackind_id. \n" \
                           "For further details contact ruth.petrie@ceda.ac.uk or the Centre for Environmental Data Analysis (CEDA) " \
                           "at support@ceda.ac.uk".format('\n'.join(errors))
        
        # Get original tracking_id
        orig_tracking_id = getattr(ncDataset(file), 'tracking_id')
        self.ncatt._run_ncatted('tracking_id', 'global', 'o', 'c', str(uuid.uuid4()), file, noHistory=True)
        self.ncatt._run_ncatted('cp4cds_update_info', 'global', 'c', 'c', cp4cds_statement, file, noHistory=True)
        self.ncatt._run_ncatted('source_tracking_id', 'global', 'c', 'c', orig_tracking_id, file, noHistory=True)
        self.ncatt._run_ncatted('history', 'global', 'a', 'c',
                           "\nUpdates made on {} were made as part of CP4CDS project see cp4cds_update_info".format(mod_date), file, noHistory=True)


    def qc_fix_wrapper(self, filepath, error_message):
        """

        A wrapper script to the individual QC error fixes

        :param filepath: full gws path
        :param error_message: error message as recorded by CF, CEDA-CC
        :return: [string] error info
        """

        fix_method, args = self.qc_mapping[error_message]
        fix = getattr(self, fix_method)
        os.chdir(os.path.dirname(filepath))
        error_info = fix(os.path.basename(filepath), error_message)

        return error_info


    def fix_cf_73b(self, file, error_message):
        """
        Fix to the CF error (7.3)

        By quick inspection it is determined that the error here is

            time: minimum (interval: 3 hours) within days time: mean over days
        it should be written
            time: minimum within days (interval: 3 hours) time: mean over days

        :param file:
        :return: [string]
        """
        variable = file.split('_')[0]
        cellMethod = self._get_cell_methods_contents(file)

        interval_before_within = re.compile("\(interval.*?within")
        if re.search(interval_before_within, cellMethod):
            _parts = cellMethod.split()
            _parts[2:4], _parts[4:7] = _parts[5:7], _parts[2:5]
            corrected_cellMethod =' '.join(_parts)
            error_info = "{} - information given in wrong order".format(error_message)
            self.ncatt._run_ncatted('cell_methods', variable, 'o', 'c', corrected_cellMethod, file)
            return error_info


    def fix_cf_73a(self, file, error_message):
        """
        A fix to CF error 7.3 with minutes typo

        :param file:
        :param error_message:
        :return: [string]
        """

        variable = file.split('_')[0]
        cellMethod = self._get_cell_methods_contents(file)
        corrected_cellMethod = cellMethod.replace('mintues', 'minutes')
        error_info = "{} - a cell_methods typo".format(error_message)
        self.ncatt._run_ncatted('cell_methods', variable, 'o', 'c', corrected_cellMethod, file)
        return error_info


    def fix_cf_31(self, file, error_message):
        """
        Fix to CF error (3.1)

        :param file:
        :param error_message:
        :return: [string]
        """

        error_info = "{} - not CF compliant units".format(error_message)
        assert file.split('_')[0] == 'sos'
        self.ncatt._run_ncatted('units', 'sos' , 'o', 'c', '1.e-3', file)
        dt_string = datetime.datetime.now().strftime('%Y-%m%d %H:%M:%S')
        methods_history_update_comment = "\n{}: CP4CDS project changed units from PSU to 1.e-3 to be CF compliant".format(dt_string)
        self.ncatt._run_ncatted('history', 'sos', 'a', 'c', methods_history_update_comment, file, noHistory=True)
        return error_info


    def fix_c4_002_005_tos(self, file, error_message):
        """
        Fix to CEDA-CC error C4.002.005 where tos has wrong standard name

        :param file:
        :param error_message:
        :return:
        """
        error_info = "Corrected error where {}".format(error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('standard_name','tos','o','c','sea_surface_temperature', file)
        return error_info


    def fix_c4_002_005_tsice(self, file, error_message):
        """
        Fix to CEDA-CC error C4.002.005 where tsice standard name is wrong
        :param file:
        :param error_message:
        :return:
        """


        assert os.path.basename(file).split('_')[0] == 'tsice'
        error_info = "Corrected error where {} : CF standard_name for tsice is [surface_temperature]".format(error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('standard_name','tsice','c','c','surface_temperature', file)
        return error_info


    def fix_c4_002_005_long_name(self, file, error_message):
        """
        Fix to CEDA-CC error C4.002.005 error in long name
        :param file:
        :param error_message:
        :return:
        """

        variable = file.split('_')[0]
        correct_longname = error_message.split(': ')[-1].strip(']').strip('"')
        error_info = "Corrected error where {} : corrected CF long_name inserted".format(error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('long_name',variable,'o','c',correct_longname, file)
        return error_info


    def fix_c4_002_005_missing_value(self, file, error_message):
        """
        Fix to CEDA-CC error C4.002.005 insert/overwrite correct missing value
        :param file:
        :param error_message:
        :return:
        """
        variable = file.split('_')[0]
        error_info = "Corrected error where {} : correct missing value of 1.0e+20f inserted".format(error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('missing_value',variable,'o','f','1.0e20', file, newfile=ofile)
        return error_info


    def fix_c4_002_007_model_id(self, file, error_message):
        """
        Fix CEDA-CC error C4.002.007 wrong model name or not correctly formatted applies to:
            [u'CESM1-CAM5-1-FV2', u'FGOALS-g2', u'ACCESS1-3']
        :param file:
        :param error_message:
        :return:
        """
        model = file.split('_')[2]
        error_info = "Corrected error where {} : corrected model_id {} inserted".format(error_message.split('::')[-1].strip(), model)
        self.ncatt._run_ncatted('model_id','global','o','c',model, file)
        return error_info

def qc_fixer(qcfile):
    """
    Run all CF and CEDA-CC fixing scripts fix all errors then write new file.
    qcfile: gws_path to file
    :return:
    """

    qcfix = QCerror_fixer()

    # with open('failed_datasets_to_fix.log') as r:
    #     datasets = r.readlines()
    #
    # for id in datasets:
    #     dfs = Dataset.objects.filter(dataset_id=id.strip()).first().datafile_set.all()
    #
    #     for df in dfs:
    #         qcerrs = df.qcerror_set.all()
    #
    # for e in qcerrs.filter(error_level__icontains='FIX', file__duplicate_of=None, error_msg="ERROR (7.3): Invalid unit mintues) in cell_methods comment"):

    df = DataFile.objects.filter(gws_path=qcfile).first()
    qcerrs = df.qcerror_set.all()
    ifile = df.gws_path
    ofile = os.path.join(NEW_DATA_DIR, os.path.basename(ifile))
    # take a copy of the file to temporary location so that inplace modifications can be made
    shutil.copy(ifile, ofile)

    all_errs = []
    for e in qcerrs:
        error_info = qcfix.qc_fix_wrapper(ofile, e.error_msg)
        all_errs.append(error_info)
    qcfix._ncatted_common_updates(ofile, all_errs)

def get_list_of_files_to_fix():
    """
    Generate a list of files with only CEDA-CC or CF errors to fix
    :return:
    """
    checks = ['QCPlot', 'LATEST', 'TIME-SERIES', 'TEMPORAL']
    dfs = DataFile.objects.filter(duplicate_of=None)

    datafiles_to_fix = set()
    for df in dfs:
        errs = df.qcerror_set.all()

        # CASE 1: Datafile has no associated move on
        if len(errs) == 0:
            continue

        # CASE 2: Errors to fix (only want CEDA-CC and CF) initially
        else:
            # CASE 2a: Error that are type: checks = ['QCPlot', 'LATEST', 'TIME-SERIES', 'TEMPORAL']
            for e in errs:
                skip = [True for c in checks if e.check_type == c]

                if skip:
                    # Not considering this dataset while it has QCPlot, LATEST, TIME-SERIES or TEMPORAL errors
                    continue

            else:
                if errs.filter(check_type='CF', error_level='FIX').count() != 0:
                    datafiles_to_fix.add(df)
                else:
                    continue
                if errs.filter(check_type='CEDA-CC', error_level='FIX').count() != 0:
                    datafiles_to_fix.add(df)
                else:
                    continue

    with open('files_to_correct_4.log', 'a+') as w:
        for f in list(datafiles_to_fix):
            w.writelines(["{}\n".format(f.gws_path)])

def count_datasets():

    with open('files_to_correct_4.log', 'a+') as r:
        files = r.readlines()

    set_of_datasets = set()
    for f in files:
        f = f.strip()

        ds = Dataset.objects.filter(datafile__gws_path=f)
        # print ds.first().dataset_id
        set_of_datasets.add(ds.first().dataset_id)

    print len(ds)
        # print set_of_datasets, type(set_of_datasets)

def check_corrected_are_ok():

    files_ok = set()

    with open('corrected_files.log') as fr:
        files = fr.readlines()

    for f in files:
        f = f.strip()

        df = DataFile.objects.filter(ncfile=os.path.basename(f)).first()
        if df:
            errs = df.qcerror_set.all()
            for e in errs:
                if not e.error_level == 'FATAL':
                    files_ok.add(f)
                    with open('corrected_ok.log') as w:
                        w.writelines(["{}\n".format(f)])

        else:
            with open('corrected_failed.log') as w:
                w.writelines(["{}\n".format(f)])


if __name__ == "__main__":

    # qcfile = argv[1]
    # qc_fixer(qcfile)
    # get_list_of_files_to_fix()
    # count_datasets()

    check_corrected_are_ok()

    # Useful for testing each error message
    # for e in QCerror.objects.filter(file__duplicate_of=None,error_msg=err)[:1]:
    # errors_by_message = [
    #                      "ERROR (7.3): Invalid unit mintues) in cell_methods comment",
    #                      "ERROR (3.1): Invalid units:  psu",
    #                      "ERROR (7.3) Invalid syntax for cell_methods attribute",
    #                      'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [tos] has incorrect attributes: standard_name="surface_temperature [correct: "sea_surface_temperature"]',
    #                      "C4.002.004: [variable_ncattribute_present]: FAILED:: Required variable attributes missing: ['standard_name']",
    #                      "C4.002.007: [filename_filemetadata_consistency]: FAILED:: File name segments do not match corresponding global attributes: [(2, 'model_id')]",
    #                      'missing_value must be present if _FillValue',
    #                      'long_name',
    #                      ]

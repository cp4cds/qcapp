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
NEW_DATA_DIR = "/group_workspaces/jasmin2/cp4cds1/data/corrected/v20180618"

class NCatted(object):

    def _run_ncatted(self, att_nm, var_nm, mode, att_type, att_val, file, newfile=None, noHistory=False):

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

        variable = os.path.basename(ifile).split('_')[0]
        ncds = ncDataset(ifile)
        v = ncds.variables[variable]
        return getattr(v, 'cell_methods')

    def _ncatted_common_updates(self, file, error_info):

        mod_date = datetime.datetime.now().strftime('%Y-%m-%d')

        cp4cds_statement = "As part of the Climate Projections for the Copernicus Data Store (CP4CDS) CMIP5 quality assurance " \
                           "testing the following error(s) were corrected by the CP4CDS team:: \n {} \n " \
                           "The tracking id was also updated; the original is stored under source_trackind_id " \
                           "For further details contact ruth.petrie@ceda.ac.uk".format(error_info)
        
        # Get original tracking_id
        orig_tracking_id = getattr(ncDataset(file), 'tracking_id')


        self.ncatt._run_ncatted('tracking_id', 'global', 'o', 'c', str(uuid.uuid4()), file, noHistory=True)
        self.ncatt._run_ncatted('cp4cds_update_info', 'global', 'c', 'c', cp4cds_statement, file, noHistory=True)
        self.ncatt._run_ncatted('source_tracking_id', 'global', 'c', 'c', orig_tracking_id, file, noHistory=True)
        self.ncatt._run_ncatted('history', 'global', 'a', 'c',
                           "\nUpdates made on {} were made as part of CP4CDS project see cp4cds_update_info".format(mod_date), file, noHistory=True)


    def qc_fix_wrapper(self, filepath, error_message):

        fix_method, args = self.qc_mapping[error_message]
        fix = getattr(self, fix_method)
        os.chdir(os.path.dirname(filepath))
        error_info = fix(os.path.basename(filepath), error_message)

        return error_info


    def fix_cf_73b(self, file, error_message):
        """
        By quick inspection it is determined that the error here is

            time: minimum (interval: 3 hours) within days time: mean over days
        it should be written
            time: minimum within days (interval: 3 hours) time: mean over days

        :param file:
        :return:
        """

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

        variable = file.split('_')[0]
        cellMethod = self._get_cell_methods_contents(file)
        corrected_cellMethod = cellMethod.replace('mintues', 'minutes')
        error_info = "{} - a cell_methods typo".format(error_message)
        self.ncatt._run_ncatted('cell_methods', variable, 'o', 'c', corrected_cellMethod, file)
        return error_info


    def fix_cf_31(self, file, error_message):

        error_info = "{} - not CF compliant units".format(error_message)
        assert file.split('_')[0] == 'sos'
        self.ncatt._run_ncatted('units', 'sos' , 'o', 'c', '1.e-3', file)
        dt_string = datetime.datetime.now().strftime('%Y-%m%d %H:%M:%S')
        methods_history_update_comment = "\n{}: CP4CDS project changed units from PSU to 1.e-3 to be CF compliant".format(dt_string)
        self.ncatt._run_ncatted('history', 'sos', 'a', 'c', methods_history_update_comment, file, noHistory=True)
        return error_info


    def fix_c4_002_005_tos(self, file, error_message):

        error_info = "Corrected error where {}".format(error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('standard_name','tos','o','c','sea_surface_temperature', file)
        return error_info


    def fix_c4_002_005_tsice(self, file, error_message):

        assert os.path.basename(file).split('_')[0] == 'tsice'
        error_info = "Corrected error where {} : CF standard_name for tsice is [surface_temperature]".format(error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('standard_name','tsice','c','c','surface_temperature', file, newfile=ofile)
        return error_info


    def fix_c4_002_005_long_name(self, file, error_message):
        variable = error_message.split(': ')[3].split(' ')[1].strip('[').strip(']')
        correct_longname = error_message.split(': ')[-1].strip(']').strip('"')
        error_info = "Corrected error where {} : corrected CF long_name inserted".format(error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('long_name',variable,'o','c',correct_longname, file, newfile=ofile)
        return error_info


    def fix_c4_002_005_missing_value(self, file, error_message):

        variable = error_message.split(' ')[-1].strip('[').strip(']')
        error_info = "Corrected error where {} : correct missing value of 1.0e+20f inserted".format(error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('missing_value',variable,'o','f','1.0e20', file, newfile=ofile)
        return error_info


    def fix_c4_002_007_model_id(self, file, error_message):
        """
        [u'CESM1-CAM5-1-FV2', u'FGOALS-g2', u'ACCESS1-3']
        :return:
        """

        model = file.split('_')[2]
        error_info = "Corrected error where {} : corrected model_id {} inserted".format(error_message.split('::')[-1].strip(), model)
        self.ncatt._run_ncatted('model_id','global','o','c',model, file, newfile=ofile)
        return error_info

    # def qc_fix_wrapper(self, filepath, error_message):
    #
    #     # print filepath, error_message
    #     # error_correction_dict = {}
    #     # error_correction_dict["ERROR (7.3): Invalid unit mintues) in cell_methods comment"] = fix_cf_73a(filepath, error_message)
    #     # error_correction_dict["ERROR (3.1): Invalid units:  psu"] = fix_cf_31(filepath, error_message)
    #     # error_correction_dict["ERROR (7.3) Invalid syntax for cell_methods attribute"] = fix_cf_73b(filepath, error_message)
    #     #
    #     # if error_message in error_correction_dict.keys():
    #     #     print error_correction_dict[error_message]
    #     #     error_correction_dict[error_message]
    #
    #
    #     if error_message == "ERROR (7.3): Invalid unit mintues) in cell_methods comment":
    #         ofile, error_info = self.fix_cf_73a(filepath, error_message)
    #     if error_message == "ERROR (3.1): Invalid units:  psu":
    #         print error_message
    #         ofile, error_info = self.fix_cf_31(filepath, error_message)
    #     if error_message == "ERROR (7.3) Invalid syntax for cell_methods attribute":
    #         ofile, error_info = self.fix_cf_73b(filepath, error_message)
    #     if error_message == 'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: ' \
    #                         'Variable [tos] has incorrect attributes: ' \
    #                         'standard_name="surface_temperature" [correct: "sea_surface_temperature"]':
    #         ofile, error_info = self.fix_c4_002_005_tos(filepath, error_message)
    #     if error_message == "C4.002.004: [variable_ncattribute_present]: FAILED:: " \
    #                         "Required variable attributes missing: ['standard_name']":
    #         ofile, error_info = self.fix_c4_002_005_tsice(filepath, error_message)
    #
    #     if error_message == "C4.002.007: [filename_filemetadata_consistency]: FAILED:: " \
    #                         "File name segments do not match corresponding global attributes: [(2, 'model_id')]":
    #         ofile, error_info = self.fix_c4_002_007_model_id(filepath, error_message)
    #     if 'missing_value must be present if _FillValue' in error_message:
    #         ofile, error_info = self.fix_c4_002_005_missing_value(filepath, error_message)
    #     if 'long_name' in error_message:
    #         ofile, error_info = self.fix_c4_002_005_long_name(filepath, error_message)
    #
    #     return error_info

    # def fix_cf_errors(self):
    #
    #     cf_errs = QCerror.objects.filter(check_type='CF', file__duplicate_of=None, error_level__startswith='FAIL')
    #     e_msgs = ["ERROR (7.3): Invalid unit mintues) in cell_methods comment",
    #               "ERROR (3.1): Invalid units:  psu",
    #               "ERROR (7.3) Invalid syntax for cell_methods attribute",
    #               ]
    #
    #     for error_message in e_msgs[2:3]:
    #         errs = cf_errs.filter(error_msg=error_message)
    #         for e in errs[:1]:
    #             fpath = e.file.gws_path
    #             same_file_errs = e.file.qcerror_set.all()
    #             multiple_errs = same_file_errs.filter(check_type='CF').exclude(check_type='CF',error_msg=error_message).filter(check_type='CEDA-CC')
    #             if len(multiple_errs) == 0:
    #                 ofile, error_info = self.cf_fix_wrapper(fpath, error_message)
    #                 self._ncatted_common_updates(ofile, error_info)
    #             else:
    #                 continue



    #
    # def do_fixes(self, error_message):

   #
   # "ERROR (7.3): Invalid unit mintues) in cell_methods comment": ('fix_cf_73a', ['filepath', 'error_message']),
   #  "ERROR (3.1): Invalid units:  psu": ('fix_cf_31',  ['filepath', 'error_message']),
   #  "ERROR (7.3) Invalid syntax for cell_methods attribute": ('fix_cf_73b', ['filepath', 'error_message']),
   #  'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: '
   #      'Variable [tos] has incorrect attributes: standard_name="surface_temperature" '
   #      '[correct: "sea_surface_temperature"]': ('fix_c4_002_005_tos',  ['filepath', 'error_message']),
   #  "C4.002.004: [variable_ncattribute_present]: FAILED:: "
   #      "Required variable attributes missing: ['standard_name']": ('fix_c4_002_005_tsice',  ['filepath', 'error_message']),
   #  "C4.002.007: [filename_filemetadata_consistency]: FAILED:: "
   #      "File name segments do not match corresponding "
   #      "global attributes: [(2, 'model_id')]": ('fix_c4_002_007_model_id',  ['filepath', 'error_message']),
   #  'missing_value must be present if _FillValue': ('fix_c4_002_005_missing_value',  ['filepath', 'error_message']),
   #  'long_name': ('fix_c4_002_005_long_name',  ['filepath', 'error_message']),


def qc_fixer():
    """
    Wrapper to all CF and CEDA-CC fixing scripts fix all errors then write new file.
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


            # for e in qcerrs.filter(file__duplicate_of=None,
            #                        error_msg="ERROR (7.3): Invalid unit mintues) in cell_methods comment")[:1]:

    for e in QCerror.objects.filter(file__duplicate_of=None,error_msg="ERROR (7.3): Invalid unit mintues) in cell_methods comment")[:1]:

                print e.error_msg
                print e.file
                ifile = e.file.gws_path
                ofile = os.path.join(NEW_DATA_DIR, os.path.basename(ifile))
                
                # Copying file allows in place changes and removes filepath from the ncatted auto-generated history att
                shutil.copy(ifile, ofile)
                
                error_info = qcfix.qc_fix_wrapper(ofile, e.error_msg)
                qcfix._ncatted_common_updates(ofile, error_info)



if __name__ == "__main__":
    qc_fixer()

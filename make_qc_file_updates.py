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

parser = argparse.ArgumentParser()
parser.add_argument('--cf_only_fixer',action='store_true', help='Check CF-errors are unique')


class QCerror_fixer(object):

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

    def _get_cell_methods_contents(self, ifile):

        variable = os.path.basename(ifile).split('_')[0]
        ncds = ncDataset(ifile)
        v = ncds.variables[variable]
        return getattr(v, 'cell_methods')

    def _ncatted_common_updates(self, ofile, error_info):

        mod_date = datetime.datetime.now().strftime('%Y-%m-%d')

        cp4cds_statement = "As part of the Climate Projections for the Copernicus Data Store (CP4CDS) CMIP5 quality assurance " \
                           "testing the following error(s) were corrected by the CP4CDS team:: \n {} \n " \
                           "The tracking id was also updated. " \
                           "For further details contact ruth.petrie@ceda.ac.uk".format(error_info)

        self._run_ncatted('tracking_id', 'global', 'o', 'c', str(uuid.uuid4()), ofile)
        self._run_ncatted('cp4cds_update_info', 'global', 'c', 'c', cp4cds_statement, ofile, noHistory=True)
        self._run_ncatted('history', 'global', 'a', 'c',
                    "Updates made on {} were made as part of CP4CDS project see cp4cds_update_info".format(mod_date), ofile)

    def fix_cf_73b(self, ifile, error_message):
        """
        By quick inspection it is determined that the error here is

            time: minimum (interval: 3 hours) within days time: mean over days
        it should be written
            time: minimum within days (interval: 3 hours) time: mean over days

        :param ifile:
        :return:
        """

        ofile = os.path.join(NEW_DATA_DIR, os.path.basename(ifile))
        cellMethod = self._get_cell_methods_contents(ifile)

        interval_before_within = re.compile("\(interval.*?within")
        if re.search(interval_before_within, cellMethod):
            _parts = cellMethod.split()
            _parts[2:4], _parts[4:7] = _parts[5:7], _parts[2:5]
            corrected_cellMethod =' '.join(_parts)
            error_info = "{} - information given in wrong order".format(error_message)
            self._run_ncatted('cell_methods', variable, 'o', 'c', corrected_cellMethod, ifile, newfile=ofile)
            self._ncatted_common_updates(ofile, error_info)


    def fix_cf_73a(self, ifile, error_message):

        ofile = os.path.join(NEW_DATA_DIR, os.path.basename(ifile))
        self.cellMethod = _get_cell_methods_contents(ifile)
        corrected_cellMethod = cellMethod.replace('mintues', 'minutes')
        error_info = "{} - a cell_methods typo".format(error_message)
        self._run_ncatted('cell_methods', variable, 'o', 'c', corrected_cellMethod, ifile, newfile=ofile)
        self._ncatted_common_updates(ofile, error_info)


    def fix_cf_31(self, ifile, error_message):

        ofile = os.path.join(NEW_DATA_DIR, os.path.basename(ifile))
        error_info = "{} - not CF compliant units".format(error_message)
        assert os.path.basename(ifile).split('_')[0] == 'sos'
        self._run_ncatted('units', 'sos' , 'o', 'c', '1.e-3', ifile, newfile=ofile)
        dt_string = datetime.datetime.now().strftime('%Y-%m%d %H:%M:%S')
        methods_history_update_comment = "\n{}: CP4CDS project changed units from PSU to 1.e-3 to be CF compliant\n".format(dt_string)
        self._run_ncatted('history', 'sos', 'a', 'c', methods_history_update_comment, ofile)
        self._ncatted_common_updates(ofile, error_info)


    def cf_fix_wrapper(self, filepath, error_message):

        # print filepath, error_message
        # error_correction_dict = {}
        # error_correction_dict["ERROR (7.3): Invalid unit mintues) in cell_methods comment"] = fix_cf_73a(filepath, error_message)
        # error_correction_dict["ERROR (3.1): Invalid units:  psu"] = fix_cf_31(filepath, error_message)
        # error_correction_dict["ERROR (7.3) Invalid syntax for cell_methods attribute"] = fix_cf_73b(filepath, error_message)
        #
        # if error_message in error_correction_dict.keys():
        #     print error_correction_dict[error_message]
        #     error_correction_dict[error_message]


        if error_message == "ERROR (7.3): Invalid unit mintues) in cell_methods comment":
            self.fix_cf_73a(filepath, error_message)
        if error_message == "ERROR (3.1): Invalid units:  psu":
            print error_message
            self.fix_cf_31(filepath, error_message)
        if error_message == "ERROR (7.3) Invalid syntax for cell_methods attribute":
            self.fix_cf_73b(filepath, error_message)


    def fix_cf_errors(self):


        cf_errs = QCerror.objects.filter(check_type='CF', file__duplicate_of=None, error_level__startswith='FAIL')
        e_msgs = ["ERROR (7.3): Invalid unit mintues) in cell_methods comment",
                  "ERROR (3.1): Invalid units:  psu",
                  "ERROR (7.3) Invalid syntax for cell_methods attribute",
                  ]

        for error_message in e_msgs[2:3]:
            errs = cf_errs.filter(error_msg=error_message)
            for e in errs[:1]:
                fpath = e.file.gws_path
                same_file_errs = e.file.qcerror_set.all()
                multiple_errs = same_file_errs.filter(check_type='CF').exclude(check_type='CF',error_msg=error_message).filter(check_type='CEDA-CC')
                if len(multiple_errs) == 0:
                    self.cf_fix_wrapper(fpath, error_message)
                else:
                    continue


if __name__ == "__main__":
    args = parser.parse_args()

    qcfix = QCerror_fixer()

    if args.cf_only_fixer:
        qcfix.fix_cf_errors()


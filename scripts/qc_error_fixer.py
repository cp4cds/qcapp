
from setup_django import *
import sys
import re
from settings import *
from utils import *
from ceda_cc import c4


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
        "ERROR (3.1): Invalid units:  psu": ('fix_cf_31', ['filepath', 'error_message']),
        "ERROR (7.3) Invalid syntax for cell_methods attribute": ('fix_cf_73b', ['filepath', 'error_message']),
        'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: '
        'Variable [tos] has incorrect attributes: standard_name="surface_temperature" '
        '[correct: "sea_surface_temperature"]': ('fix_c4_002_005_tos', ['filepath', 'error_message']),
        "C4.002.004: [variable_ncattribute_present]: FAILED:: "
        "Required variable attributes missing: ['standard_name']": (
        'fix_c4_002_005_tsice', ['filepath', 'error_message']),
        "C4.002.007: [filename_filemetadata_consistency]: FAILED:: "
        "File name segments do not match corresponding "
        "global attributes: [(2, 'model_id')]": ('fix_c4_002_007_model_id', ['filepath', 'error_message']),
        'missing_value must be present if _FillValue': ('fix_c4_002_005_missing_value', ['filepath', 'error_message']),
        'long_name': ('fix_c4_002_005_long_name', ['filepath', 'error_message']),
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
                                "\nUpdates made on {} were made as part of CP4CDS project see cp4cds_update_info".format(
                                    mod_date), file, noHistory=True)

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
            corrected_cellMethod = ' '.join(_parts)
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
        self.ncatt._run_ncatted('units', 'sos', 'o', 'c', '1.e-3', file)
        dt_string = datetime.datetime.now().strftime('%Y-%m%d %H:%M:%S')
        methods_history_update_comment = "\n{}: CP4CDS project changed units from PSU to 1.e-3 to be CF compliant".format(
            dt_string)
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
        self.ncatt._run_ncatted('standard_name', 'tos', 'o', 'c', 'sea_surface_temperature', file)
        return error_info

    def fix_c4_002_005_tsice(self, file, error_message):
        """
        Fix to CEDA-CC error C4.002.005 where tsice standard name is wrong
        :param file:
        :param error_message:
        :return:
        """

        assert os.path.basename(file).split('_')[0] == 'tsice'
        error_info = "Corrected error where {} : CF standard_name for tsice is [surface_temperature]".format(
            error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('standard_name', 'tsice', 'c', 'c', 'surface_temperature', file)
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
        error_info = "Corrected error where {} : corrected CF long_name inserted".format(
            error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('long_name', variable, 'o', 'c', correct_longname, file)
        return error_info

    def fix_c4_002_005_missing_value(self, file, error_message):
        """
        Fix to CEDA-CC error C4.002.005 insert/overwrite correct missing value
        :param file:
        :param error_message:
        :return:
        """
        variable = file.split('_')[0]
        error_info = "Corrected error where {} : correct missing value of 1.0e+20f inserted".format(
            error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('missing_value', variable, 'o', 'f', '1.0e20', file, newfile=ofile)
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
        error_info = "Corrected error where {} : corrected model_id {} inserted".format(
            error_message.split('::')[-1].strip(), model)
        self.ncatt._run_ncatted('model_id', 'global', 'o', 'c', model, file)
        return error_info



def fix_errors(variable, frequency, table):

    qcfix = QCerror_fixer()

    for experiment in ALLEXPTS:
        
        datafiles = DataFile.objects.filter(dataset__variable=variable, dataset__frequency=frequency, 
                                            dataset__cmor_table=table, dataset__experiment=experiment)
        
        for df in datafiles:
            qc_errors = df.qcerror_set.all()
            if qc_errors:
                for e in qcerrs:
                    # 27th sept got to here.. move to new location in archive...
                    ifile = df.gws_path
                    ofile = os.path.join(NEW_DATA_DIR, os.path.basename(ifile))
                    # take a copy of the file to temporary location so that inplace modifications can be made
                    shutil.copy(ifile, ofile)

                    error_info = qcfix.qc_fix_wrapper(ofile, e.error_msg)
                    all_errs.append(error_info)
                qcfix._ncatted_common_updates(ofile, all_errs)



if __name__ == "__main__":

    variable = sys.argv[1]
    frequency = sys.argv[2]
    table = sys.argv[3]

    fix_errors(variable, frequency, table)
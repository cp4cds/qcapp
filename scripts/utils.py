
import os
import datetime
import re
import glob
import requests
import itertools
import json
import uuid
from netCDF4 import Dataset as ncDataset
from subprocess import call
from settings import *





def get_institute_from_model(model):

    for ins, models in model_dict.iteritems():
        if model in models:
            return ins

    return None

def get_frequency_from_table(t):

    for freq, table in table_freq_mapping.iteritems():
        if t in table:
            return freq

    return None

def get_realm_from_table(t):

    for realm, table in table_realm_mapping.iteritems():
        if t in table:
            return realm

    return None

def get_and_parse_json(url):

    resp = requests.get(url, verify=False)
    json_resp = resp.json()
    results = json_resp["response"]["docs"]
    return results


def write_json(file, json_content):
    with open(file, 'w+') as fw:
        json.dump(json_content, fw)


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
        'C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [prsn] has incorrect attributes: long_name="Solid Precipitation" [correct: "Snowfall Flux"]': ('fix_c4_002_005_prsn', ['filepath', 'error_message']),
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

    def get_cell_methods_contents(self, ifile):
        """
        From the input file get the cell methods content

        :param ifile:
        :return: [string] cell method content
        """
        variable = os.path.basename(ifile).split('_')[0]
        ncds = ncDataset(ifile)
        v = ncds.variables[variable]
        return getattr(v, 'cell_methods')

    def ncatted_common_updates(self, file, errors):
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
        cellMethod = self.get_cell_methods_contents(file)

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
        cellMethod = self.get_cell_methods_contents(file)
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

    def fix_c4_002_005_prsn(self, file, error_message):
        """
        Fix to CEDA-CC error C4.002.005 where tos has wrong standard name

        :param file:
        :param error_message:
        :return:
        """
        error_info = "Corrected error where {}".format(error_message.split('::')[-1].strip())
        self.ncatt._run_ncatted('long_name', 'prsn', 'o', 'c', 'Snowfall Flux', file)
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



def check_log_exists(file, qcdir, ext):

    qcfiles = os.listdir(qcdir)
    cc_logfile = "{}{}".format(os.path.basename(file).strip('.nc'), ext)
    for f in qcfiles:
        if f.startswith(cc_logfile):
            return True, f
    return False, ""



def get_and_make_logdir(datafile, force_version=None):
    print(datafile.gws_path)
    adaf
    inst, model, expt, freq, realm, table, ensemble, var, ver, ncfile = datafile.gws_path.split('/')[7:]
    if force_version:
        v_version = 'v{}'.format(force_version)
    else:
        if ver == 'latest':
            v_version = os.readlink(os.path.dirname(datafile.gws_path))
        else:
            v_version = ver
    logdir = os.path.join(QCLOGS, var, table, expt, ensemble, v_version)

    if not os.path.isdir(logdir):
        os.makedirs(logdir)

    return logdir

def read_json(file):
    """

    From a given url this routine returns the elements from ["response"]["docs"]

    :param url:
    :return:
    """
    with open(file) as r:
        jsn = r.read()

    data = json.loads(jsn)
    res = data["response"]["docs"]
    return res


def define_local_json_cache_names(variable, frequency, table, experiment):

    json_logdir = os.path.join(LOCAL_JSON_DF_CACHE)

    if not os.path.isdir(json_logdir):
        os.makedirs(json_logdir)

    logfile = "{}_{}_{}_{}.json".format(variable, frequency, table, experiment)
    json_file = os.path.join(json_logdir, logfile)

    return json_logdir, json_file

def convert_to_cp4cds_gws_path(ipath, dir1, dir2):
    path = ipath.replace(dir1, dir2)
    path_list = path.split('/')
    version = path_list[-2]
    if not version.startswith('v'):
        version = "v"+version
        path_list[-2] = version
    if path_list[-3] == "files":
        path_list.pop(-3)
        gws_path = "/".join(path_list)
        return gws_path


    path_list[-3], path_list[-2] = path_list[-2], path_list[-3]
    gws_path = "/".join(path_list)
    return gws_path


def get_start_end_times(frequency, fname):
    """
    Get start and end times from the filename
    :return:
    """

    if fname.endswith('.nc'):

        ncfile = os.path.basename(fname)
        timestamp = ncfile.strip('.nc').split('_')[-1]

        # IF timestamp is of the form YYYYMMDDHHMM-YYYYMMDDHHMM
        if len(timestamp) == 25:
            start_time = datetime.date(int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[7:8]))
            end_time = datetime.date(int(timestamp[-12:-8]), int(timestamp[-8:-6]), int(timestamp[-6:-4]))

        # IF timestamp is of the form YYYYMMDDHH-YYYYMMDDHH
        if len(timestamp) == 21:
            start_time = datetime.date(int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[7:8]))
            end_time = datetime.date(int(timestamp[-10:-6]), int(timestamp[-6:-4]), int(timestamp[-4:-2]))

        if frequency == 'mon':
            start_time = datetime.date(int(fname[-16:-12]), int(fname[-12:-10]), 01)
            end_mon = fname[-5:-3]
            if end_mon == '02':
                end_day = 28
            elif end_mon in ['04', '06', '09', '11']:
                end_day = 30
            else:
                end_day = 31
            end_time = datetime.date(int(fname[-9:-5]), int(fname[-5:-3]), end_day)

        if frequency == 'day':
            start_time = datetime.date(int(fname[-20:-16]), int(fname[-16:-14]), int(fname[-14:-12]))
            end_time = datetime.date(int(fname[-11:-7]), int(fname[-7:-5]), int(fname[-5:-3]))
    else:
        start_time = datetime.date(1900, 1, 1)
        end_time = datetime.date(1999, 12, 31)

    return start_time, end_time


def _generate_datafile_url(fname):

    template = "https://esgf-index1.ceda.ac.uk/esg-search/search/?type=File&" \
               "latest=true&distrib=true&project=CMIP5&title={}&format=application%2Fsolr%2Bjson".format(fname)

    return template

def is_timeseries(filepath):
    """

    Checks whether the file is part of a timeseries by checking whether it
    exists as a single file in its directory.

    Returns True if only file in the directory
    Returns False if there is more than one file in the directory.

    :param filepath: valid filepath
    TODO: Add in valid filepath check
    :return: Boolean
    """

    if os.path.isdir(os.path.dirname(filepath)):

        if len(os.listdir(os.path.dirname(filepath))) > 1:
           ts = True
        else:
           ts = False
    else:
        ts = None

    return ts


def get_start_end_times(frequency, fname):
    """

    From a file name e.g. tas_Amon_EC-EARTH_historical_r13i1p1_200001-200911.nc
    The final element here is the file temporal range.

    Currently only working with monthly and daily data and so only returning a date object

    TODO Improve this to cope with 3 and 6 hourly data.

    If the temporal element is monthly then it has only YYYY and MM but no DD component as required for a
    datetime.date object. Irrespective of calendar used in the data a standard calendar is assumed and a dummy DD is
    generated in order that a datetime.date object can be generated.

    TODO: Incorporate the calendar information(?)

    :param frequency: CMIP5 frequency
    :param fname: filename
    :return: tuple of datetime.date objects representing the start and end times
    """


    if fname.endswith('.nc'):

        ncfile = os.path.basename(fname)
        timestamp = ncfile.strip('.nc').split('_')[-1]

        # IF timestamp is of the form YYYYMMDDHHMM-YYYYMMDDHHMM
        if len(timestamp) == 25:
            start_time = datetime.date(int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[7:8]))
            end_time = datetime.date(int(timestamp[-12:-8]), int(timestamp[-8:-6]), int(timestamp[-6:-4]))

        # IF timestamp is of the form YYYYMMDDHH-YYYYMMDDHH
        if len(timestamp) == 21:
            start_time = datetime.date(int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[7:8]))
            end_time = datetime.date(int(timestamp[-10:-6]), int(timestamp[-6:-4]), int(timestamp[-4:-2]))

        if frequency == 'mon':
            start_time = datetime.date(int(fname[-16:-12]), int(fname[-12:-10]), 01)
            end_mon = fname[-5:-3]
            if end_mon == '02':
                end_day = 28
            elif end_mon in ['04', '06', '09', '11']:
                end_day = 30
            else:
                end_day = 31
            end_time = datetime.date(int(fname[-9:-5]), int(fname[-5:-3]), end_day)

        if frequency == 'day':
            start_time = datetime.date(int(fname[-20:-16]), int(fname[-16:-14]), int(fname[-14:-12]))
            end_time = datetime.date(int(fname[-11:-7]), int(fname[-7:-5]), int(fname[-5:-3]))
    else:
        start_time = datetime.date(1900, 1, 1)
        end_time = datetime.date(1999, 12, 31)

    return start_time, end_time


def generate_filelist(FILELIST):
    """

    Generate a full list of all files in the QC db
    This is a debugging function and does not run in parallel context

    :param FILELIST: A global variable
    """
    # Ensure output file exists
    call(['touch', FILELIST])

    with open(FILELIST, 'a') as fw:
        for df in DataFiles.objects.all():
            fw.writelines([df.archive_path, "\n"])



def convert_archivepath_to_gwspath(arch_path):

    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = arch_path.split('/')[6:]
    alpha_base = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1"
    gws_path = os.path.join(alpha_base, institute, model, experiment, frequency, realm, table, ensemble, variable,
                            'latest', ncfile)
    return gws_path


def clear_cedacc_ouptut():
    """
    Tidy up any ceda-cc output files
    Move ceda-cc output files to a log_dir

    :return:
    """

    # Ensure log_dir exists
    logdir = os.path.join(QCAPP_PATH, 'log_dir')
    if not os.path.isdir(logdir):
        os.makedirs(logdir)

    # List of ceda-cc output files
    cedacc_ofiles = ["cccc_atMapLog.txt",
                     "amapDraft.txt"
                     "Rec.json",
                     "Rec.txt"]

    # If CEDA-CC output exists put this into a log_dir
    for f in cedacc_ofiles:
        filepath = os.path.join(QCAPP_PATH, f)
        if os.path.isfile(filepath):
            mv_cmd = ['mv', filepath, logdir]
            res = call(mv_cmd)

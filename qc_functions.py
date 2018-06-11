import django

django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

import collections
import os
import shutil
import timeit
import datetime
import time
import re
import glob
import commands
import requests, itertools
from subprocess import call
from netCDF4 import Dataset as ncDataset
from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version
from time_checks.run_file_timechecks import main as single_file_time_checks
# from time_checks.run_multifile_timechecks import main as multi_file_time_checks
from esgf_dict import EsgfDict
from qc_settings import *
from is_latest import check_datafile_is_latest


def update_for_missing_cf_records(fpath, odir):

    cf_logfile = os.path.join(odir, os.path.basename(fpath).replace(".nc", ".cf-log.txt"))
    if not os.path.exists(cf_logfile):
        print "LOG FILE MISSING {}".format(fpath)
        print "RUNNING CF CHECKER"
        run_cf_checker(fpath, odir)
        df = DataFile.objects.filter(gws_path=fpath).first()
        print "PARSING CF CHECKER"
        parse_cf_checker(df, fpath, odir)

        if os.path.exists(cf_logfile):
            print "LOG FILE EXISTS REMOVING RECORD"
            e = QCerror.objects.filter(check_type='CF', error_msg='NO CF-LOG FILE',
                                       file__gws_path=fpath).first()
            e.delete()


def update_cf_qc_error_record():

    logdir_base = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/QC_LOGS/"
    cf_errs = QCerror.objects.filter(check_type='CF')

    cfe = cf_errs.values_list('error_msg', flat=True).distinct()

    for e in cfe:
        print "{} :: {}".format(e, cf_errs.filter(error_msg=e).count())

    # DONE - ASSIGNED
    error_msg_level_dict = {}
    error_msg_level_dict["ERROR (7.3): Invalid unit mintues) in cell_methods comment"] = "FAIL"
    error_msg_level_dict["ERROR (5): co-ordinate variable 'time' not monotonic"] = "FATAL :: NOT FIXABLE"
    error_msg_level_dict["ERROR (5): co-ordinate variable 'lon' not monotonic"] = "FATAL :: NOT FIXABLE"
    error_msg_level_dict["ERROR (5): co-ordinate variable 'plev' not monotonic"] = "FATAL :: NOT FIXABLE"
    error_msg_level_dict["ERROR (4): Axis attribute is not allowed for auxillary coordinate variables."] = "INFO :: IGNORING"
    error_msg_level_dict["ERROR (5): co-ordinate variable 'lat' not monotonic"] = "FATAL :: NOT FIXABLE"
    error_msg_level_dict["ERROR (3.1): Invalid units:  psu"] = "FAIL"
    error_msg_level_dict["ERROR (7.3) Invalid syntax for cell_methods attribute"] = "FAIL"

    # for e in error_msg_level_dict.keys():
    #     print "{} :: {}".format(e, cf_errs.filter(error_msg=e).count())

    # missing = cf_errs.filter(error_msg='NO CF-LOG FILE')
    #
    # print missing.count()
    # for i in missing:
    #
    #     df = i.file
    #     ins, model, exp, freq, realm, table, ens, version, var, ncfile = df.archive_path.split('/')[6:]
    #     odir = os.path.join(logdir_base, var, table, exp, ens, version)
    #     cf_logfile = os.path.join(odir, ncfile.replace(".nc", ".cf-log.txt"))
    #     if not os.path.exists(cf_logfile):
    #
    #         print "LOG FILE MISSING {}".format(cf_logfile)
    #
    #     else:
    #         print "DELETING ERROR RECORD LOG FILE PRESENT {}".format(cf_logfile)
    #         i.delete()


def resolve_cedacc_exceptions():

    print "resolve cedacc exceptions"
    logdir_base = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/QC_LOGS/"
    cc_errs = QCerror.objects.filter(check_type='CEDA-CC', error_msg='C4.100.001: [exception]: FAILED:: Exception has occured')

    for err in cc_errs[1:]:
        file = err.file.archive_path

        institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = file.split('/')[6:]
        logdir = os.path.join(logdir_base, variable, table, experiment, ensemble, version)
        logdir_files = os.listdir(logdir)
        df = err.file

        for f in logdir_files:
            print "f {}".format(f)

            if f.startswith(ncfile.replace('.nc', '__qclog_')):
                cedacc_file = os.path.join(logdir, f)

                if os.path.exists(cedacc_file):
                    print "exists removing redo ceda-cc and parse {}".format(cedacc_file)
                    os.remove(cedacc_file)

                print "QC file {}".format(file)
                # print "logdir {}".format(logdir)
                # print "DO CEDA CC"
                cedacc_args = ['ceda-cc', '-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', logdir, '--cae',
                               '--blfmode', 'a', ]
                run_cedacc = call(cedacc_args)
                err.delete()
                break


    # e = cc_errs.first()
    # file = e.file.gws_path
    # print e.file.gws_path
    # odir = '.'
    # cedacc_args = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', odir, '--cae', '--blfmode', 'a', ]
    # run_cedacc = c4.main(cedacc_args)


def update_cedacc_qc_errors():

    cc_errs = QCerror.objects.filter(check_type='CEDA-CC')

    get_values_list = True
    if get_values_list:
        cce = cc_errs.values_list('error_msg', flat=True).distinct()
        for e in cce:
            print "{} :: {}".format(e, cc_errs.filter(error_msg=e).count())

    # for e in cc_errs.filter(error_msg__contains='Exception'):
    #     print e.report_filepath
    #     print e.file.archive_path


    cf_error_msg_level_dict = {}

    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [tos] has incorrect attributes: standard_name="surface_temperature" [correct: "sea_surface_temperature"]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [uas] has incorrect attributes: long_name="Eastward Near-Surface Wind" [correct: "Eastward Near-Surface Wind Speed"]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [od550aer] has incorrect attributes: long_name="Ambient Aerosol Opitical Thickness at 550 nm" [correct: "Ambient Aerosol Optical Thickness at 550 nm"]'] = ""
    cf_error_msg_level_dict["C4.002.004: [variable_ncattribute_present]: FAILED:: Required variable attributes missing: ['standard_name']"] = ""
    cf_error_msg_level_dict['loggedException'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [vas] has incorrect attributes: long_name="Northward Near-Surface Wind Speed" [correct: "Northward Near-Surface Wind"]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [evspsbl]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [rsdt]'] = ""
    cf_error_msg_level_dict['C4.100.001: [exception]: FAILED:: Exception has occured'] = ""
    cf_error_msg_level_dict['raise loggedException'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [ps]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [prsn] has incorrect attributes: long_name="Solid Precipitation" [correct: "Snowfall Flux"]'] = ""
    cf_error_msg_level_dict["C4.002.007: [filename_filemetadata_consistency]: FAILED:: File name segments do not match corresponding global attributes: [(2, 'model_id')]"] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [tas]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [uas] has incorrect attributes: long_name="Eastward Near-Surface Wind Speed" [correct: "Eastward Near-Surface Wind"]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [psl]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [sos] has incorrect attributes: units="1.e-3" [correct: "psu"]'] = ""
    cf_error_msg_level_dict['Exception has occured'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [mrsos] has incorrect attributes: long_name="Moisture in Upper 0.1 m of Soil Column" [correct: "Moisture in Upper Portion of Soil Column"]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [sos] has incorrect attributes: units="1" [correct: "psu"]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [tasmax]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: Variable [vas] has incorrect attributes: long_name="Northward Near-Surface Wind" [correct: "Northward Near-Surface Wind Speed"]'] = ""
    cf_error_msg_level_dict['C4.002.005: [variable_ncattribute_mipvalues]: FAILED:: missing_value must be present if _FillValue is [pr]'] = ""
                        

def create_new_dataset_records():

    errs = QCerror.objects.filter(check_type__contains='LATEST', error_msg__contains='VERSION ERROR',
                                  error_level__contains='INFO [UPDATED]')

    for e in errs[1:]:
        fname = e.file.gws_path
        print fname
        variable = fname.split('/')[-1].split('_')[0]
        version = "v{}".format(os.readlink(e.file.gws_path).split('/')[-2])
        drs = e.file.dataset.esgf_drs
        drs_parts = drs.split('.')
        drs_parts[0] = drs_parts[0].upper()
        drs_parts.append(variable)
        drs_parts.append(version)
        dsid = '.'.join(drs_parts)

        if e.file.dataset.version == version:
            pass

        elif Dataset.objects.filter(dataset_id=dsid).exists:
            print "EXISTS"
            e.file.dataset = Dataset.objects.filter(dataset_id=dsid).first()
            e.file.save()

        else:
            print "NEW"
            orig_ds = e.file.dataset
            new_ds = Dataset.objects.get(pk=e.file.dataset.pk)
            new_ds.pk = None
            new_ds.id = None
            new_ds.version = version
            new_ds.supersedes = orig_ds
            new_ds.save()

            e.file.dataset = orig_ds
            e.file.save()

def parse_is_latest_version_error(msg):

    err, checksums, versions, query = msg.split(' :: ')
    cksum_parts = checksums.split(' ')
    ceda_cksum = cksum_parts[4]
    latest_cksum = cksum_parts[-1]

    version_parts = versions.split(' ')
    ceda_version = version_parts[2]
    latest_version = version_parts[6]

    return ceda_cksum, latest_cksum, ceda_version, latest_version

def make_new_version(logfile, error, ceda_version, latest_version):

    gwsPath = error.file.gws_path
    gwsdir = os.path.dirname(gwsPath).strip('latest')

    # Update file paths
    os.chdir(gwsdir)
    if not os.path.exists(ceda_version):
        message = "CEDA DIR DOESN'T EXIST {}/{}".format(gwsdir, ceda_version)
        _save_errorobj_message(error, message, logfile, print_msg=True)
    else:
        # Make the new version directory for all the files
        if not os.path.exists(latest_version):
            shutil.copytree(ceda_version, latest_version, symlinks=True)
            os.remove('latest')
            os.symlink(latest_version, 'latest')

        # update the db
        message = "INFO [UPDATED] :: CEDA VERSION :: {} UPDATED TO LATEST VERSION {} :: FILE {}".format(
            ceda_version, latest_version, error.file)
        _save_errorobj_message(error, message, logfile)

        #
        # if not error.file.dataset.version == latest_version:
        #     new_ds = Dataset.objects.get(pk=error.file.dataset.pk)
        #     new_ds.pk = None
        #     new_ds.id = None
        #     new_ds.version = latest_version
        #     new_ds.save()
        # else:
        #     new_ds = error.file.dataset

        error.file.new_dataset_version = True
        # error.file.dataset = new_ds
        error.file.save()


def _save_errorobj_message(eobj, message, logfile, print_msg=False):

    eobj.error_level = message
    eobj.save()
    if print_msg == True:
        with open(logfile, 'a+') as w:
            w.writelines(message)





def run_is_latest(variable, frequency, table):

    esgf_dict = EsgfDict([
        ("node", "esgf-index1.ceda.ac.uk"),
        ("project", "CMIP5"),
        ("frequency", frequency),
        ("table", table),
        ("variable", variable),
        ("distrib", "true"),
        ("latest", "true"),
    ])

    for experiment in ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']:
    # for experiment in ['historical']:
        esgf_dict['experiment'] = experiment
        datafiles = DataFile.objects.filter(variable=variable, dataset__frequency=frequency,
                                            dataset__cmor_table=table, dataset__experiment=experiment)
        check_datafile_is_latest(datafiles, esgf_dict)


def run_multifile_time_checker(datasets, var, table, expt):

    # ds = datasets.first()
    for ds in datasets:
        if ds.datafile_set.count() > 1:
            for d in ds.datafile_set.all():
                d.timeseries = True
                d.save()

            df = ds.datafile_set.first()
            ensemble = df.gws_path.split('/')[-4]
            version = "v" + os.readlink(df.gws_path).split('/')[-2]
            odir = os.path.join(QCLOGS, var, table, expt, ensemble, version)
            if not os.path.isdir(odir):
                os.makedirs(odir)

            f = os.path.basename(df.gws_path).strip('.nc').split('_')
            ofile = '_'.join(f[:-1]) + '__multifile_timecheck.log'
            logfile = os.path.join(odir, ofile)

            dir_of_files = os.path.dirname(df.gws_path)
            files = os.listdir(dir_of_files)
            filelist = []

            for f in files:
                if f.endswith('.nc'):
                    filelist.append(os.path.join(dir_of_files, f))

            multi_file_time_checks(filelist, logfile)


def file_time_checks(ifile, odir):
    """

    :param ifile: no path information
    :param odir:
    :return:
    """

    logfile = os.path.join(odir, ifile.replace('.nc', '__file_timecheck.log'))
    try:
        d = ncDataset(ifile)
    except(IOError):
        d = None

    if isinstance(d, ncDataset):
        if not os.path.exists(logfile):
            single_file_time_checks(ifile, odir)
    else:

        with open(logfile, 'w') as fw:
            fw.writelines(["Time checks of: {} \n".format(ifile)])
            fw.writelines(["T0.000::[FATAL]::Not a NetCDF file"])


def run_ceda_cc(file, odir):
    """

    Runs CEDA-CC on the input file

    :param file: valid filepath to run CEDA-CC
    :return:
    """
    if not os.path.exists(file):
        ofile = ncfile.replace('.nc', '__cedacc_error.log')
        with open(ofile, 'w+') as fw:
            err_message = "{} : Does not exist \n".format(file)
            fw.writelines(err_message)
    else:
        institute, model, experiment, frequency, realm, table, ensemble, variable, version, ncfile = file.split('/')[8:]
        ofile = ncfile.strip('.nc')
        now = datetime.datetime.now().strftime('%Y%m%d')
        ofile = "{}__qclog_{}.txt".format(ofile, now)
        if os.path.exists(ofile):
            print "{} exists not performing ceda-cc on {}".format(ofile, file)
        else:
            cedacc_args = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', odir, '--cae', '--blfmode', 'a',]
            run_cedacc = c4.main(cedacc_args)


def parse_ceda_cc(df_obj, odir):
    """
    Parses the CEDA-CC output on the input file.

    Finds any errors recorded by CEDA-CC and then makes a QCerror record for each found.

    :param dbobj:
    :param odir:
    :return:
    """

    checkType = "CEDA-CC"
    file_path = df_obj.gws_path
    temporal_range = file_path.split("_")[-1].strip(".nc").split("_")[0]
    institute, model, experiment, frequency, realm, table, ensemble, variable, latest, ncfile = file_path.split('/')[8:]
    file_base = "_".join([variable, table, model, experiment, ensemble, temporal_range])

    # Constructs a CEDA-CC regex based on variable_table_model_experiment_ensemble_temporal-range__qclog_{date}.txt
    ceda_cc_file_pattern = re.compile(file_base + "__qclog_\d+\.txt")

    # List files in the CEDA-CC logdir
    # log_dir = os.path.join(CEDACC_DIR,  institute, model, experiment, frequency, realm, version)
    log_dir_files = os.listdir(odir)

    for logfile in log_dir_files:

        # If the input file is in the logdir parse the output
        if ceda_cc_file_pattern.match(logfile):
            ceda_cc_file = os.path.join(odir, logfile)
            with open(ceda_cc_file, 'r') as fr:
                ceda_cc_out = fr.readlines()

            # Identify where CEDA-CC picks up a QC error
            cedacc_global_error = re.compile('.*global.*FAILED::.*')
            cedacc_variable_error = re.compile('.*variable.*FAILED::.*')
            cedacc_other_error = re.compile('.*filename.*FAILED::.*')
            cedacc_exception = re.compile('.*FAILED:: Exception has occured.*')
            cedacc_abort = re.compile('.*(aborted|ABORTED).*')

            # For CEDA-CC ouput search for errors and if found make a QCerror record
            for line in ceda_cc_out:
                line = line.strip()
                if cedacc_global_error.match(line):
                    make_qc_err_record(df_obj, checkType, "global", line, ceda_cc_file)
                if cedacc_variable_error.match(line):
                    make_qc_err_record(df_obj, checkType, "variable", line, ceda_cc_file)
                if cedacc_other_error.match(line.strip()):
                    make_qc_err_record(df_obj, checkType, "other", line, ceda_cc_file)
                if cedacc_exception.match(line):
                    make_qc_err_record(df_obj, checkType, "fatal", line, ceda_cc_file)
                if cedacc_abort.match(line):
                    make_qc_err_record(df_obj, checkType, "fatal", line, ceda_cc_file)


def run_cf_checker(file, odir):
    """

    Run the CF-Checker on the input file from the shell by calling out using subprocess.call

    :param file: GWS NetCDF file
    """

    # Define output and error log files
    cf_out_file = os.path.join(odir, os.path.basename(file).replace(".nc", ".cf-log.txt"))
    cf_err_file = os.path.join(odir, os.path.basename(file).replace(".nc", ".cf-err.txt"))
    run_cmd = ["/usr/bin/cf-checker", "-a", AREATABLE, "-s", STDNAMETABLE, "-v", "auto", file]
    cf_out, cf_err = open(cf_out_file, "w"), open(cf_err_file, "w")
    call(run_cmd, stdout=cf_out, stderr=cf_err)
    cf_out.close(), cf_err.close()

    if os.path.getsize(cf_err_file) == 0:
        os.remove(cf_err_file)
    else:
        filen = file.replace('.nc', '.cf-err')
        filename = os.path.join(CF_FATAL_DIR, filen)
        touch_cmd = ["touch", filename]
        call(touch_cmd)


def parse_cf_checker(df, file, log_dir):
    """

    Parses the CF-Checker output for the input file

    Finds any errors recorded by the CF-Checker and then makes a QCerror record for each found.

    :param file: Archive file
    TODO: check it is a valid file?

    :return:
    """

    # CF regex expressions for errors
    cf_global_error = re.compile('.*ERROR.*(global|Global|Convention).*')
    cf_variable_error = re.compile('.*ERROR.*(units|cell).*(?!.*(time|boundary|coordinate|co-ordinate)).*')
    cf_other_error = re.compile('.*ERROR.*(bound|Boundary|grid|coordinate|co-ordinate|dimension).*')
    cf_abort = re.compile('.*suffix.*')
    cf_fatal = re.compile('.*COULD NOT OPEN FILE.*')
    # Dictionary mapping the CF regex with type of error
    regexlist = [(cf_global_error, "global"),
                 (cf_variable_error, "variable"),
                 (cf_other_error, "other"),
                 (cf_abort, "fatal"),
                 (cf_fatal, "fatal")
                 ]

    checkType = "CF"

    temporal_range = file.split("_")[-1].strip(".nc")
    institute, model, experiment, frequency, realm, table, ensemble, variable, latest, ncfile = file.split('/')[8:]
    file_base = "_".join([variable, table, model, experiment, ensemble, temporal_range])

    # Constructs a CF file regex based on variable_table_model_experiment_ensemble_temporal-range.cf-log.txt
    cf_log = file_base + ".cf-log.txt"
    cf_file_pattern = re.compile(cf_log)
    
    # List files in the CF logdir
    # log_dir = os.path.join(CF_DIR, institute, model, experiment, frequency, realm, version)
    log_dir_files = os.listdir(log_dir)

    cf_out = None
    found = False
    for logfile in log_dir_files:

        # If the input file is in the logdir parse the output
        if cf_file_pattern.match(logfile):
            found = True
            with open(os.path.join(log_dir, logfile), 'r') as fr:
                cf_out = fr.readlines()

            for line in cf_out:
                line = line.strip()
                for regex, label in regexlist:
                    if regex.search(line):
                        make_qc_err_record(df, checkType, label, line, os.path.join(log_dir, logfile))

    if not found:
        make_qc_err_record(df, checkType, 'FATAL', 'NO CF-LOG FILE', os.path.join(log_dir, cf_log))



#
# cf_error_levels = {}
# cf_error_levels['ERROR (7.3): Invalid unit mintues) in cell_methods comment'] = 'WARN'
# cf_error_levels["ERROR (5): co-ordinate variable 'time' not monotonic"] = 'FATAL'
# cf_error_levels["ERROR (5): co-ordinate variable 'lon' not monotonic"] =  'FATAL'
# cf_error_levels["ERROR (5): co-ordinate variable 'plev' not monotonic"] = 'FATAL'
# cf_error_levels['ERROR (4): Axis attribute is not allowed for auxillary coordinate variables.'] = 'INFO'
# cf_error_levels["ERROR (5): co-ordinate variable 'lat' not monotonic"] = 'FATAL'
# cf_error_levels['ERROR (3.1): Invalid units:  psu'] = 'WARN'
# cf_error_levels['ERROR (7.3) Invalid syntax for cell_methods attribute'] = 'WARN'
# cf_error_levels['COULD NOT OPEN FILE, PLEASE CHECK THAT NETCDF IS FORMATTED CORRECTLY.'] = 'FATAL'


def make_qc_err_record(dfile, checkType, errorType, errorMessage, filepath, errorLevel=None):

    qc_err, _ = QCerror.objects.get_or_create(file=dfile,
                                              check_type=checkType,
                                              error_type=errorType,
                                              error_msg=errorMessage,
                                              report_filepath=filepath,
                                              error_level=errorLevel
                                             )

    # TODO: Must add in a test for a non-zero .cf-err.txt and record perhaps retry or read in only here


def parse_singlefile_timechecks(df_obj, log_dir):
    """
    Parses the CEDA-CC output on the input file.

    Finds any errors recorded by CEDA-CC and then makes a QCerror record for each found.

    :param df_obj: Model datafile object
    :param log_dir:
    :return:
    """

    checkType = "TEMPORAL"
    file_path = df_obj.gws_path
    temporal_range = file_path.split("_")[-1].strip(".nc").split("_")[0]
    institute, model, experiment, frequency, realm, table, ensemble, variable, latest, ncfile = file_path.split('/')[8:]
    file_timecheck_filename = "_".join(
        [variable, table, model, experiment, ensemble, temporal_range]) + "__file_timecheck.log"
    file_timecheck_logfile = os.path.join(log_dir, file_timecheck_filename)

    file_temporal_fatal = re.compile('.*FATAL.*|.*File does not end with.*')
    file_temporal_fail = re.compile('.*FAIL.*')

    file_regexlist = [(file_temporal_fatal, "fatal"),
                      (file_temporal_fail, "fail")]

    if os.path.exists(file_timecheck_logfile):
        with open(file_timecheck_logfile, 'r') as fr:
            file_timecheck_data = fr.readlines()

            for line in file_timecheck_data:
                for regex, label in file_regexlist:
                    if regex.match(line.strip()):
                        make_qc_err_record(df_obj, checkType, label, line, file_timecheck_logfile)
    else:
        make_qc_err_record(df_obj, checkType, "FATAL", "Timecheck log file does not exist", file_timecheck_logfile)


def parse_multifile_timechecks(ds_obj, log_dir):
    """
    Parses the CEDA-CC output on the input file.

    Finds any errors recorded by CEDA-CC and then makes a QCerror record for each found.

    :param df_obj: Model datafile object
    :param log_dir:
    :return:
    """

    checkType = "TEMPORAL"
    file_path = ds_obj.gws_path
    temporal_range = file_path.split("_")[-1].strip(".nc").split("_")[0]
    institute, model, experiment, frequency, realm, table, ensemble, variable, latest, ncfile = file_path.split('/')[8:]
    multifile_timecheck_filename = "_".join([variable, table, model, experiment, ensemble]) + "__multifile_timecheck.log"
    multifile_timecheck_logfile = os.path.join(log_dir, multifile_timecheck_filename)


    multifile_temporal_fatal = re.compile('.*Error.*')
    multifile_temporal_fail = re.compile('.*FAIL.*')

    multifile_regexlist = [(multifile_temporal_fatal, "fatal"),
                 (multifile_temporal_fail, "fail")]

    if os.path.exists(multifile_timecheck_logfile):
        with open(multifile_timecheck_logfile, 'r') as fr:
            multifile_timecheck_data = fr.readlines()

            for line in multifile_timecheck_data:
                for regex, label in multifile_regexlist:
                    if regex.match(line.strip()):
                        make_qc_err_record(df_obj, checkType, label, line, multifile_timecheck_logfile)
    else:
        make_qc_err_record(df_obj, checkType, "FATAL", "Multifile timecheck log file does not exist", multifile_timecheck_logfile)



# def max_timeseries_qc_errors(ts):
#     """
#     Input is of the format of a dictionary of dictonary e.g.
#     {'filename': {'global': 0, 'variable': 1, 'other', 1}}
#     :param ts:
#     :return:
#     """
#
#     max_errors = {'global': 0, 'variable': 0, 'other': 0}
#
#     for key in ['global', 'variable', 'other']:
#         errors = []
#         for file, errs in ts.iteritems():
#             errors.append(errs[key])
#         max_errors[key] = max(errors)
#
#     return max_errors
#
#
# def get_total_qc_errors(qcfile):
#     files = DataFile.objects.filter(ncfile=qcfile)
#     # if files != 1:
#     #    raise Exception("Length of files %s must not be greater than 1, length is %s: " % (qcfile, len(files)))
#
#     file = files.first()
#     qc_errors = file.qcerror_set.all()
#     errors = {}
#     errors['global'] = qc_errors.filter(error_type='global').count()
#     errors['variable'] = qc_errors.filter(error_type='variable').exclude(error_msg__contains="ERROR (4)").count()
#     errors['other'] = qc_errors.filter(error_type='other').exclude(error_msg__contains="ERROR (4)").count()
#
#     return errors
#
#
#
# def get_list_of_qc_files():
#
#     for dataset in Dataset.objects.all():
#         datafiles = dataset.datafile_set.all()
#         for dfile in datafiles:
#             qc_errors = dfile.qcerror_set.all()
#             for error in qc_errors:
#                 path = error.file.archive_path
#                 file = error.file.ncfile


def update_dataset_versions():

    logfile = "dataset_version_update_error.log"
    errors = QCerror.objects.filter(error_msg__contains='VERSION ERROR').exclude(file__duplicate_of=True)

    done = ['/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CMS/rcp85/mon/atmos/Amon/r1i1p1/ps/latest/ps_Amon_CMCC-CMS_rcp85_r1i1p1_208001-208912.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/ICHEC/EC-EARTH/rcp45/mon/atmos/Amon/r13i1p1/ps/latest/ps_Amon_EC-EARTH_rcp45_r13i1p1_201001-201012.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CESM/historical/mon/atmos/Amon/r1i1p1/ps/latest/ps_Amon_CMCC-CESM_historical_r1i1p1_187001-187412.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/ICHEC/EC-EARTH/rcp45/mon/atmos/Amon/r13i1p1/ps/latest/ps_Amon_EC-EARTH_rcp45_r13i1p1_202801-202812.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp45/mon/atmos/Amon/r3i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp45_r3i1p3_210101-212512.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp85/mon/atmos/Amon/r2i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp85_r2i1p3_205101-207512.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CMS/rcp85/mon/atmos/Amon/r1i1p1/ts/latest/ts_Amon_CMCC-CMS_rcp85_r1i1p1_207001-207912.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-H/rcp85/mon/atmos/Amon/r1i1p3/ts/latest/ts_Amon_GISS-E2-H_rcp85_r1i1p3_225101-230012.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CESM/piControl/mon/atmos/Amon/r1i1p1/ts/latest/ts_Amon_CMCC-CESM_piControl_r1i1p1_439001-439512.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp26/mon/atmos/Amon/r1i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp26_r1i1p3_220101-222512.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp26/mon/atmos/Amon/r1i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp26_r1i1p3_207601-210012.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CESM/historical/mon/atmos/Amon/r1i1p1/ps/latest/ps_Amon_CMCC-CESM_historical_r1i1p1_187001-187412.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/ICHEC/EC-EARTH/rcp45/mon/atmos/Amon/r13i1p1/ps/latest/ps_Amon_EC-EARTH_rcp45_r13i1p1_202801-202812.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/ICHEC/EC-EARTH/rcp45/mon/atmos/Amon/r13i1p1/ps/latest/ps_Amon_EC-EARTH_rcp45_r13i1p1_202801-202812.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp45/mon/atmos/Amon/r3i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp45_r3i1p3_210101-212512.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CMS/rcp85/mon/atmos/Amon/r1i1p1/ts/latest/ts_Amon_CMCC-CMS_rcp85_r1i1p1_207001-207912.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-H/rcp85/mon/atmos/Amon/r1i1p3/ts/latest/ts_Amon_GISS-E2-H_rcp85_r1i1p3_225101-230012.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/CMCC/CMCC-CMS/rcp85/mon/atmos/Amon/r1i1p1/ps/latest/ps_Amon_CMCC-CMS_rcp85_r1i1p1_208001-208912.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/piControl/mon/atmos/Amon/r1i1p1/tas/latest/tas_Amon_GISS-E2-R_piControl_r1i1p1_435601-438012.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp45/mon/atmos/Amon/r3i1p1/tas/latest/tas_Amon_GISS-E2-R_rcp45_r3i1p1_215101-217512.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp45/mon/atmos/Amon/r3i1p1/tas/latest/tas_Amon_GISS-E2-R_rcp45_r3i1p1_217601-220012.nc',
            '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/NASA-GISS/GISS-E2-R/rcp45/mon/atmos/Amon/r3i1p3/ts/latest/ts_Amon_GISS-E2-R_rcp45_r3i1p3_210101-212512.nc',
            ]

    for error in errors:
        if not error.file in done:

            print error.file
            ceda_cksum, latest_cksum, ceda_version_no, latest_version_no = parse_is_latest_version_error(error.error_msg)
            ceda_version = "v{}".format(ceda_version_no)
            latest_version = "v{}".format(latest_version_no)

            # ENSURE CHECKSUMS ARE THE SAME
            if ceda_cksum == latest_cksum:

                # CHECK THE VERSION IS NEWER
                if len(ceda_version_no) == 1 and len(latest_version_no) ==1:
                    if int(ceda_version_no) < int(latest_version_no):
                        make_new_version(logfile, error, ceda_version, latest_version)
                    else:
                        message = "INFO [NO UPDATE] :: CEDA VERSION :: {} IS GREATER THAN OR EQUAL TO LATEST {} :: FILE {}".format(
                            ceda_version_no, latest_version_no, error.file)
                        _save_errorobj_message(error, message, logfile)

                elif len(ceda_version_no) == 8 and len(latest_version_no) == 8:

                    if datetime.datetime.strptime(ceda_version_no, '%Y%m%d') < datetime.datetime.strptime(latest_version_no, '%Y%m%d'):
                        make_new_version(logfile, error, ceda_version, latest_version)
                    else:
                        message = "INFO [NO UPDATE] :: CEDA VERSION :: {} IS GREATER THAN OR EQUAL TO LATEST {} :: FILE {}".format(
                            ceda_version_no, latest_version_no, error.file)
                        _save_errorobj_message(error, message, logfile)

                else:
                    message = "FAIL [VERSION TYPES INCOMPATIBLE] :: CEDA VERSION {} :: LATEST VERSION {} :: " \
                                "FILE {}".format(ceda_version, latest_version, error.file)
                    _save_errorobj_message(error, message, logfile, print_msg=True)
            else:
                message = "FAIL [CHECKSUM MATCH] :: CEDA CHECKSUM {} :: LATEST CHECKSUM {} :: " \
                            "FILE {}".format(ceda_cksum, latest_cksum, error.file)
                _save_errorobj_message(error, message, logfile, print_msg=True)

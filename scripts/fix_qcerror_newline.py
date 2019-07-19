
from setup_django import *
import re
import os
import shutil


def make_ds_qcerr_record(dset, checkType, errorType, errorMessage, filepath, errorLevel=None):
    qc_err, _ = QCerror.objects.get_or_create(set=dset,
                                              check_type=checkType,
                                              error_type=errorType,
                                              error_msg=errorMessage,
                                              report_filepath=filepath,
                                              error_level=errorLevel
                                              )


def parse_timeseries_log(ds_obj):

    checkType = "TIME-SERIES"
    qclogdir = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/QC_LOGS/'
    # ds = df_obj.dataset
    df_first = ds_obj.datafile_set.first()
    variable, realm, experiment, ensemble, version = df_first.variable, ds_obj.cmor_table, ds_obj.experiment, \
                                                     ds_obj.ensemble, ds_obj.version.strip()

    filename = df_first.ncfile.replace('.nc', '__file_timecheck.log')
    logdir = os.path.join(qclogdir, variable, realm, experiment, ensemble, version)
    logfile = os.path.join(logdir, filename)
    # logfile = os.path.join(logdir, "pr_day_IPSL-CM5A-LR_amip_r2i1p1__multifile_timecheck.log")
    # print os.path.exists(logfile), logfile

    # if not os.path.exists(logfile):
    #     print("No single file time check performed ", df_obj)
    # else:

    file_temporal_fatal = re.compile('.*FATAL.*|.*File does not end with.*')
    file_temporal_fail = re.compile('.*FAIL.*')
    file_regexlist = [(file_temporal_fatal, "fatal"),
                      (file_temporal_fail, "fail")]

    with open(logfile, 'r') as fr:
        file_timecheck_data = [line.strip() for line in fr]

    # df_errors = list(df_obj.qcerror_set.filter(check_type=checkType))
    # print df_errors
    for line in file_timecheck_data:
        for regex, errType in file_regexlist:
            if regex.match(line):
                # if not checkType in df_errors:
                print "Will make a qc error record ", ds_obj, checkType, errType, line
                # print "Will make a qc error record ",df_obj, checkType, errType, line, logfile
                make_ds_qcerr_record(ds_obj, checkType, errType, line, logfile)
                return False

    return True

def main():

    with open('timeseries-error-logs-to-check.log') as r:
        datasets = [line.strip() for line in r]

    for dsid in datasets:
        ds = Dataset.objects.filter(dataset_id=dsid).first()
        parse_timeseries_log(ds)


        # ds = err.set
        #
        # if not ts_qc_status:
        #     for df in ds.datafile_set.all():
        #         if not 'cmip5_raw' in df.gws_path:
        #             print "dataset in wrong dir"
        #


if __name__ == "__main__":
    
    main()
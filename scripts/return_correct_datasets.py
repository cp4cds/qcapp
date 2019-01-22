

"""

This script look in the cmip5_raw directory and moves any datasets that have no qc error into the /alpha/c3scmip5/
qc'd directory, it removes the erroneous new version directory also.

It makes associated changes to the database.

Not for general use.

"""

from setup_django import *
import sys
import re
import shutil
from settings import *
from utils import *

# from ceda_cc import c4
# from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version
# from time_checks.run_file_timechecks import main as single_file_time_checks
# from time_checks.run_multifile_timechecks import main as multi_file_time_checks
# from is_latest import check_datafile_is_latest
# import subprocess
# from utils import *
# from qc_functions import *
# from is_latest import *
#
QC_PASSED_BASE = '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5'
QC_FAILED_BASE = '/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw'
ERROR_LOG = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/move_ok_files_to_qcpassed.log'
NEW_VERSION = 'v20181201'

def move_dataset_to_qcd_dir(dataset):

    ds = Dataset.objects.filter(dataset_id=dataset).first()
    ds_dir = '/'.join(ds.datafile_set.first().gws_path.split('/')[:-2])
    files2018_dir = os.path.join(ds_dir, 'files', '20181201')

    # REMOVE 2018 files dir
    if os.path.isdir(files2018_dir):
        shutil.rmtree(files2018_dir)

    qc_gws_dir = ds_dir.replace(QC_FAILED_BASE, QC_PASSED_BASE)

    if os.path.exists(qc_gws_dir):
        with open(ERROR_LOG, 'a+') as w:
            w.writelines("PATH EXISTS {}\n".format(qc_gws_dir))
        return

    # MOVE TO QC'd DIR
    shutil.move(ds_dir, qc_gws_dir)

    # Correct datafile record paths
    datafiles = ds.datafile_set.all()
    for df in datafiles:
        df.gws_path = df.gws_path.replace(QC_FAILED_BASE, QC_PASSED_BASE)
        df.save()

    # DELETE v2018 dataset record
    dsid_2018 = '.'.join(dataset.split('.')[:-1]) + '.' + NEW_VERSION
    ds_2018 = Dataset.objects.filter(dataset_id=dsid_2018).first()
    if ds_2018:
        ds_2018.delete()

def get_ok_datasets_list():

    not_published = Dataset.objects.exclude(published=True).exclude(version='v20181201')

    for dataset in not_published:

        qc_failed = False

        for df in dataset.datafile_set.all():
            errs = df.qcerror_set.all()
            if len(errs) > 0:
                qc_failed = True
                break

        if qc_failed:
            continue
        else:
            dataset.qc_passed=True
            dataset.save()
            with open('../ancil_files/ok_datasets.log', 'a+') as w:
                w.writelines('{}\n'.format(dataset))


if __name__ == "__main__":

    # get_ok_datasets_list()

    with open('../ancil_files/ok_datasets.log') as r:
        data = r.readlines()

    for d in data:
        dataset = d.strip()

        ds = Dataset.objects.filter(dataset_id=dataset).first()
        df_path = ds.datafile_set.first().gws_path
        if 'alpha' in df_path:
            continue
        print dataset
        move_dataset_to_qcd_dir(dataset)

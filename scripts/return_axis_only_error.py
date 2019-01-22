
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
ERROR_LOG = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/move_qc_passed_files_to_qc_dir_21012019.log'
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
    ds_id_2018 = '.'.join(dataset.split('.')[:-1]) + '.' + NEW_VERSION
    ds_2018 = Dataset.objects.filter(dataset_id=ds_id_2018).first()
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
            dataset.qc_passed =True
            dataset.save()
            with open('../ancil_files/ok_datasets.log', 'a+') as w:
                w.writelines('{}\n'.format(dataset))


if __name__ == "__main__":

    with open('../ancil_files/files_2018_dirs.log') as r:
        data = r.readlines()

    for dir in data[1:]:
        dir = dir.strip()
        nfiles = len(os.listdir(dir))

        if nfiles == 0:
            print "removing {}".format(dir)
            shutil.rmtree(dir)

            inst, model, expt, frq, realm, table, ens, var = dir.split('/')[7:-2]
            ds_id  = '.'.join([inst, model, expt, frq, realm, table, ens, var]) + '.'
            orig_ds = Dataset.objects.filter(dataset_id__icontains=ds_id).exclude(version='v20181201')
            if len(orig_ds) != 1:
                print("ERROR: original ds not found {}".format(ds_id))
                continue

            ds_2018 = Dataset.objects.filter(dataset_id__icontains=ds_id, version='v20181201')

            if len(ds_2018) == 1:
                ds_2018 = ds_2018.first()
                print "DELETE DATASET {} Num datafiles {}".format(ds_2018, len(ds_2018.datafile_set.all()))
                ds_2018.delete()
                print(ds_id)
            else:
                print("No v20181201 dataset found {}".format(ds_id))


            dir = '/'.join(dir.split('/')[:-2])

            qcd_dir = dir.replace(QC_FAILED_BASE, QC_PASSED_BASE)

            if os.path.exists(qcd_dir):
                print "PATH EXISTS {}\n".format(qcd_dir)
                with open(ERROR_LOG, 'a+') as w:
                    w.writelines("PATH EXISTS {}\n".format(qcd_dir))

            else:
                # MOVE TO QC'd DIR
                print "MOVING: {} {}".format(dir, qcd_dir)
                shutil.move(dir, qcd_dir)

            print "PUBLISH {}".format(orig_ds.first().dataset_id)
            with open('../ancil_files/qc_passed_publish_ok_21012019.log', 'a+') as w:
                w.writelines('{}\n'.format(orig_ds.first().dataset_id))







        # Sort at datafile level
        #
        # files = os.listdir(dir)
        # file = files[0]
        # orig_ds = DataFile.objects.filter(ncfile=file).exclude(dataset__version='v20181201').first().dataset
        # orig_ds_version = orig_ds.version
        # o_datafiles = orig_ds.datafile_set.all()

        # for f in o_datafiles:
        #     errors = f.qcerror_set.all()
        #     unique_errors = set()
        #     for e in errors:
        #         unique_errors.add(e.error_msg)
        #
        #     delete_file = False
        #     if len(list(unique_errors)) == 1:
        #         if 'ERROR (4): Axis attribute' in list(unique_errors)[0]:
        #             delete_file = True
        #
        #     if delete_file:
        #         if not f.gws_path.startswith('/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/'):
        #             print("ERROR: Not in raw dir {}".format(f.gws_path))
        #             continue
        #
        #         file_filesdir = f.gws_path.replace('latest','files/20181201')
        #
        #         if os.path.exists(file_filesdir):
        #             df_2018 = DataFile.objects.filter(gws_path=f.gws_path).filter(dataset__version='v20181201')
        #             if len(df_2018) == 1:
        #                 df_2018 = df_2018.first()
        #                 print("DELETE FILE {}".format(file_filesdir))
        #                 os.remove(file_filesdir)
        #                 print("DELETE DATAFILE DB RECORD {}".format(df_2018))
        #                 df_2018.delete()
        #             else:
        #                 print("ERROR: DATAFILE RECORD DOES NOT EXIST {}".format(f))
        #         # else:
        #         #     print("FILE DOES NOT EXIST {}".format(file_filesdir))

                # remove 2018 file
                # remove df - 2018 record

        # if len(os.listdir(dir)) == 0:
        #     print "Delete 2018 dir {}".format(dir)
        #     ds_2018 = DataFile.objects.filter(ncfile=file, dataset__version='v20181201').first()
        #     if len(ds_2018):
        #         print("DELETING DS RECORD {}".format(ds_2018))
        #     else:
        #         print("ERROR v2018 RECORD doesn't exist {}".format())

        # check dataset is empty
                # remove 2018 dir
                # remove 2018 ds record
                # move back to qc'd dir







    #
    # for d in data:
    #     dataset = d.strip()
    #
    #     ds = Dataset.objects.filter(dataset_id=dataset).first()
    #     df_path = ds.datafile_set.first().gws_path
    #     if 'alpha' in df_path:
    #         continue
    #     print dataset
    #     move_dataset_to_qcd_dir(dataset)

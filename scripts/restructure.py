
from setup_django import *
import os
import sys
import shutil

datasets_in_psql = '../ancil_files/c3s-cmip5_dataset_versions_in_psql.log'
FIX_DB_QC_FLAG = False
QC_PASSED_BASE = '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5'
QC_FAILED_BASE = '/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw'


def get_list_of_datasets(file):

    with open(file) as r:
        data = r.readlines()

    return data


def fix_drs(datasets):

    data = []
    for ds in datasets:
        ds = ds.strip().replace('c3s-cmip5', 'CMIP5')
        data.append(ds)
    return data


def set_qc_flag_true(datasets):

    for dsid in datasets:

        ds_obj = Dataset.objects.filter(dataset_id=dsid)
        if ds_obj:
            assert(len(ds_obj) == 1)
            ds_obj = ds_obj.first()
            ds_obj.qc_passed = True
            ds_obj.save()

        else:
            with open('fails2.log', 'a+') as w:
                w.writelines(["{}\n".format(dsid)])


def fix_db_status():
    datasets = get_list_of_datasets(datasets_in_psql)
    datasets = fix_drs(datasets)
    set_qc_flag_true(datasets)


def update_database(df):

    new_gws_path = df.gws_path.replace(QC_PASSED_BASE, QC_FAILED_BASE)
    if not os.path.islink(new_gws_path):
        return False
    # assert(os.path.islink(new_gws_path))
    df.gws_path = new_gws_path
    df.save()

    return True


def move_failed_dataset(dataset):

    # for ds in datasets:
        # if ds.dataset_id == "CMIP5.output1.CSIRO-QCCCE.CSIRO-Mk3-6-0.rcp26.mon.atmos.Amon.r9i1p1.ta.v1":
        #     print "skipping CMIP5.output1.CSIRO-QCCCE.CSIRO-Mk3-6-0.rcp26.mon.atmos.Amon.r9i1p1.ta.v1"
        #     continue

    ds = Dataset.objects.filter(dataset_id__startswith=dataset).first()

    print "DATASET ", ds

    if ds.qc_passed:
        print "QC PASSED NOT TO BE REMOVED"
        return

    datafiles = ds.datafile_set.all()

    df1 = datafiles.first()
    src = os.path.dirname(df1.gws_path).strip('latest').rstrip('/')
    dst = os.path.dirname(df1.gws_path).strip('latest').rstrip('/').replace(QC_PASSED_BASE, QC_FAILED_BASE)
    if not os.path.exists(src):
        print "SRC already removed {}".format(src)
        return

    if os.path.exists(dst):
        print "DST EXISTS {}".format(dst)
        return

    print "SRC ", src
    print "DST ", dst
    print "COPYING"
    shutil.copytree(src, dst, symlinks=True)

    print "REMOVE ", src
    shutil.rmtree(src)

    for df in datafiles:
        updated = update_database(df)
        if not updated:
            print "DATABASE UPDATE FAILED {}".format(df.gws_path)


def main():

    if FIX_DB_QC_FLAG:
        fix_db_status()

    failed_datasets = Dataset.objects.exclude(qc_passed=True)

    move_failed_datasets_data(failed_datasets)


if __name__ == "__main__":

    dataset = sys.argv[1]
    move_failed_dataset(dataset)
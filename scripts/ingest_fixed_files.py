
from setup_django import *
import sys
import re
import shutil
import json
import subprocess
from settings import *
from utils import *
# /group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/ICHEC/EC-EARTH/rcp45/mon/atmos/Amon/r6i1p1/uas/

QC_PASSED_BASE = '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5'
QC_FAILED_BASE = '/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw'
PUBLISH_LOG = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/publish_to_esgf.log'
ERROR_LOG = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/ingest_fixed_files_error_log.log'


def write_error_log(dataset_id, error_msg):
    print error_msg
    with open(ERROR_LOG, 'a+') as w:
        w.writelines(["{} : {}\n".format(error_msg, dataset_id)])


def get_or_create_new_dataset_record(ds, new_version_no):

    new_drs_version = '.'.join(ds.dataset_id.split('.')[:-1]) + '.v' + new_version_no
    nds_query = Dataset.objects.filter(dataset_id=new_drs_version)

    if len(nds_query) == 0:
        new_ds = ds
        new_ds.pk = None
        new_ds.version = 'v{}'.format(new_version_no)
        new_ds.save()
        new_ds.supersedes = ds
        new_ds.save()
        return new_ds
    else:
        return nds_query.first()


def get_or_create_new_datafile_record(df, nds, new_version_no):

    d = DataFile.objects.filter(gws_path=df.gws_path)
    for i in d:
        if i.dataset.version.lstrip('v') == new_version_no:
            return i

    new_df = df
    new_df.pk = None
    # link to new dataset
    new_df.dataset = nds
    new_df.save()
    new_df.supersedes = df
    new_df.qc_fixed = True
    new_df.save()

    return new_df


def check_dataset_is_complete(fixed_ds):

    version = fixed_ds.version
    base_ds_id = '.'.join(fixed_ds.dataset_id.split('.')[:-1])
    orig_ds = Dataset.objects.filter(dataset_id__startswith=base_ds_id).exclude(version=version).first()

    if not orig_ds:
        return False, []

    orig_files = orig_ds.datafile_set.all()
    fixed_files = fixed_ds.datafile_set.all()

    if len(orig_files) == len(fixed_files):
        return True, []

    else:
        missing_files = set()
        orig_files = set(orig_files)
        fixed_files = set(fixed_files)
        missing_files = orig_files - fixed_files

        missing_ok = check_missing_files_ok(missing_files, fixed_ds)
        if missing_ok:
            return True, missing_files
        else:
            return False, []


def check_missing_files_ok(missing_files, fixed_ds):

    print missing_files
    if len(list(missing_files)) == 0:
        return True

    for f in list(missing_files):

        nc = os.path.basename(str(f))
        df = DataFile.objects.filter(ncfile=nc).exclude(dataset__version='v20181201').first()
        errs = df.qcerror_set.all()
        errors = set()
        for e in errs:
            errors.add(e.error_msg)

        errors_list = list(errors)
        if len(errors_list) == 1:

            if not errors_list[0].startswith('ERROR (4): Axis attribute'):
                write_error_log(fixed_ds, 'FILE NOT FIXED')
                return False

            # if errors_list[0].startswith('ERROR (4): Axis attribute'):
            #     copy_ok = copy_file_to_v2018_dir(df)
            #     if not copy_ok:
            #         write_error_log(fixed_ds, 'FAILED TO COPY TO NEW VERSION')
            #         return False
            #     missing_files.remove(df)
            # else:
            #     write_error_log(fixed_ds, 'FILE NOT FIXED')
            #     return False

        else:
            write_error_log(fixed_ds, 'FILE NOT FIXED')
            return False

        # nds = get_or_create_new_dataset_record(fixed_ds, '20181201')
        # df = get_or_create_new_datafile_record(df, nds, '20181201')


def copy_file_to_v2018_dir(df):

    basepath = '/'.join(df.gws_path.split('/')[:-2])
    orig_version = os.readlink(os.path.join(basepath, 'latest')).strip('v')
    orig_path = os.path.join(basepath, 'files', orig_version)
    new_path = os.path.join(basepath, 'files', "20181201")
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    print "MOVING {} {}".format(orig_path, new_path)

    shutil.copy(orig_path, new_path)

    if os.path.exists(new_path):
        return True
    else:
        return False


def move_completed_dataset(ds):

    basepath = '/'.join(ds.datafile_set.first().gws_path.split('/')[:-2])
    rawdir = os.path.join(basepath, 'files', "20181201")
    qc_dir = os.path.join(basepath.replace(QC_FAILED_BASE, QC_PASSED_BASE), 'files', "20181201")
    if os.path.exists(qc_dir):
        return False

    make_new_latest_dir(basepath)
    shutil.move(basepath, basepath.replace(QC_FAILED_BASE, QC_PASSED_BASE))
    print basepath.replace(QC_FAILED_BASE, QC_PASSED_BASE)
    return True


def make_new_latest_dir(basepath):

    os.chdir(basepath)
    if 'latest' in os.listdir('.'):
        os.remove('latest')
    os.makedirs('v20181201')
    files2018dir = os.path.join('files', '20181201')
    files = os.listdir(files2018dir)
    
    os.chdir('v20181201')

    for file in files:
        os.symlink(os.path.join('..', 'files', '20181201', file), file)

    os.chdir('../')
    os.symlink('v20181201', 'latest')


def update_datafile_records(ds):

    ds_id_base = '.'.join(ds.dataset_id.split('.')[:-1])
    datasets = Dataset.objects.filter(dataset_id__icontains=ds_id_base)
    new_dsid = ds_id_base + ".v20181201"

    for d in datasets:
        if new_dsid in d.dataset_id:
            new_ds = d
        else:
            old_ds = d

    new_ds.qc_passed = True
    new_ds.supersedes = old_ds
    new_ds.save()

    if not len(old_ds.datafile_set.all()) == len(new_ds.datafile_set.all()):
        write_error_log(ds, "FATAL NUMBER OF DATAFILES DOES NOT MATCH")
        return False


    datafiles = new_ds.datafile_set.all()
    for df in datafiles:

        new_path = df.gws_path.replace(QC_FAILED_BASE, QC_PASSED_BASE)

        if not os.path.exists(new_path):
            write_error_log(ds, "FATAL NO NEW PATH")
            return False

        df.gws_path = new_path
        df.save()

    return True


def write_dataset_id_for_publication(ds):

    # print "WRITING DSID {}".format(ds.dataset_id)

    with open(PUBLISH_LOG, 'a+') as w:
        w.writelines(["{}\n".format(ds.dataset_id)])


def ingest_fixed_dataset(datasetID):

    ds = Dataset.objects.filter(dataset_id=datasetID).first()

    dataset_is_complete, files_not_in_new_version = check_dataset_is_complete(ds)
    if not dataset_is_complete:
        write_error_log(ds.dataset_id, "DATASET NOT COMPLETE")
        return
    if dataset_is_complete:
        print files_not_in_new_version
        print len(files_not_in_new_version)
    stopa
    print "dataset_is_complete", dataset_is_complete


    moved = move_completed_dataset(ds)
    if not moved:
        write_error_log(ds.dataset_id, "MOVE FILES FAILED")
        return

    print "moved", moved

    df_records_updated = update_datafile_records(ds)
    if not df_records_updated:
        write_error_log(ds.dataset_id, "DATABASE RECORDS NOT UPDATED")
        return

    print "df_records_updated", df_records_updated

    # write_dataset_id_for_publication(ds)


if __name__ == "__main__":
    skip = [
        'CMIP5.output1.ICHEC.EC-EARTH.rcp45.mon.atmos.Amon.r6i1p1.uas.v20181201',
        # MISSING FILES FROM ARCHIVE - restructure.py
        # 'CMIP5.output1.CMCC.CMCC-CMS.piControl.mon.seaIce.OImon.r1i1p1.sim.v20181201',
        # Dataset not complete
        # ERROR IN THE DATASET RECORD 2018 ok but time errors linked to original record
    ]

    # var = sys.argv[1]
    # # datasetID = sys.argv[1]
    # # print datasetID
    #
    # dfs = DataFile.objects.filter(gws_path__startswith='/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/',
    #                               dataset__version='v20181201', variable=var)
    # datasets = set()
    #
    # for df in dfs:
    #     datasets.add(df.dataset.dataset_id)


    for datasetID in ['CMIP5.output1.CMCC.CMCC-CMS.piControl.mon.seaIce.OImon.r1i1p1.sim.v20181201',]:
    # for datasetID in list(datasets):
        if datasetID in skip:
            continue
        ingest_fixed_dataset(datasetID)

    # DONE DELETE AFTER NEXT COMMIT
    # dataset_records_to_fix = [
    #     'CMIP5.output1.CSIRO-BOM.ACCESS1-3.rcp85.mon.seaIce.OImon.r1i1p1.sim.v20181201',
    #     # New file fixed but not set as latest on file system, supersedes not correctly set, qc_passed not set
    #     'CMIP5.output1.CSIRO-BOM.ACCESS1-3.rcp45.mon.seaIce.OImon.r1i1p1.sim.v20181201',
    #     # New file fixed but not set as latest on file system,supersedes not correctly set, qc_passed not set
    #     'CMIP5.output1.CSIRO-BOM.ACCESS1-3.historical.mon.seaIce.OImon.r3i1p1.sim.v20181201',
    #     # New file fixed but not set as latest on file system, supersedes not correctly set, qc_passed not set
    #     'CMIP5.output1.CSIRO-BOM.ACCESS1-3.historical.mon.seaIce.OImon.r2i1p1.sim.v20181201',
    #     # New file fixed but not set as latest on file system, supersedes not correctly set, qc_passed not set
    # ]



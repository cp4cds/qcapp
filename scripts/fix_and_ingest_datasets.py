
from setup_django import *
import sys
import re
import shutil
import json
import subprocess
import netCDF4 as nc
import file_error_fixer as filefixer
from settings import *
from utils import *
# /group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/ICHEC/EC-EARTH/rcp45/mon/atmos/Amon/r6i1p1/uas/

QC_PASSED_BASE = '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5'
QC_FAILED_BASE = '/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw'
PUBLISH_LOG = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/to_publish_to_esgf_2019-01-28.log'
ERROR_LOG = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/logfiles/fix_and_ingest_fixed_files_error_log.log'
DIRS_TO_FIX = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/datasets_to_fix_2019-01-24.log'
DATASETS_TO_FIX_DIR = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/status_logs/fix_2019-01-29/to_fix'
DATASETS_IN_PROGRESS_DIR = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/status_logs/fix_2019-01-29/in_progress'
DATASETS_DONE_PUBLISH_DIR = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/status_logs/fix_2019-01-29/done_publish'
DATASETS_FAILED_TO_COMPLETE_DIR = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/status_logs/fix_2019-01-29/failed_to_complete'
DATASETS_WONT_FIX_DIR = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/status_logs/fix_2019-01-29/remaining_qc_errors'
NEW_VERSION_NO = '20181201'
NEW_VERSION = 'v20181201'

def write_error_log(dataset_id, error_msg):
    print error_msg
    with open(ERROR_LOG, 'a+') as w:
        w.writelines(["{} : {}\n".format(error_msg, dataset_id)])



def move_directory_to_qcd_dir(ods):

    src = os.path.dirname(ods.datafile_set.first().gws_path).replace('latest', '')
    dst = src.replace(QC_FAILED_BASE, QC_PASSED_BASE)

    print "MOVING {} {}".format(src, dst)
    if os.path.exists(dst):
        return False
    else:
        shutil.move(src, dst)
        if not os.path.exists(dst):
            return False

    return True

def create_new_version(ods):

    dsdir = os.path.dirname(ods.datafile_set.first().gws_path).replace('latest', '')
    oversion = ods.version.strip('v')

    ofilesdir = os.path.join(dsdir, 'files', oversion)
    nfilesdir = os.path.join(dsdir, 'files', NEW_VERSION_NO)
    new_version_dir = os.path.join(dsdir, NEW_VERSION)

    if not len(os.listdir(ofilesdir)) == len(os.listdir(nfilesdir)):
        write_error_log(ods, 'FAIL number of files on system do not match')
        return False

    os.chdir(dsdir)
    print os.getcwd()
    if not os.path.isdir(NEW_VERSION):
        print "  remove latest",
        os.remove('latest')
        print "  make dir ", NEW_VERSION
        os.makedirs(NEW_VERSION)
        print "  symlink latest"
        os.symlink(NEW_VERSION, 'latest')

    newFiles = os.listdir(ofilesdir)
    for file in newFiles:
        os.chdir(new_version_dir)
        src = os.path.join('..', 'files', NEW_VERSION_NO, file)
        if os.path.islink(src):
            src = src.replace(NEW_VERSION_NO, oversion)
        dst = file

        print "  Linking {} {}".format(src, dst)
        os.symlink(src, dst)
    return True


def check_datafile_fixed(orig_ds, datafile):

    ncfile = nc.Dataset(datafile)
    try:
        cp4cds_glb_att = getattr(ncfile, 'cp4cds_update_info')
    except(AttributeError):
        write_error_log(orig_ds, "ERROR Datafile has not been corrected {}".format(datafile))
        return "Not fixed"

    if not cp4cds_glb_att: return "Wrong fix"
    elif "  \n" in cp4cds_glb_att:
        write_error_log(orig_ds, "ERROR Datafile has been corrected with no errors {}".format(datafile))
        return "Wrong fix"
    else:
        return "ok"

def create_new_dataset_record(ods):

    new_ds = ods
    new_ds.pk = None
    new_ds.version = NEW_VERSION
    new_ds.save()

    return new_ds


def get_or_create_new_datafile_record(odf):


    d = DataFile.objects.filter(gws_path=odf.gws_path, dataset__version='v20181201').first()
    if d:
        return d
    else:
        new_df = odf
        new_df.pk = None
        new_df.save()

    return new_df


def update_database_records(ods):

    nds = Dataset.objects.filter(dataset_id__icontains='.'.join(ods.dataset_id.split('.')[:-1]), version='v20181201')

    if not nds:
        nds = create_new_dataset_record(ods)
    elif len(nds) != 1:
        write_error_log(ods, "More than two dataset records match query")
        return False
    else:
        nds = nds.first()

    nds.qc_passed = True
    nds.supersedes = ods
    nds.save()


    odfs = ods.datafile_set.all()

    for odf in odfs:
        ndf = get_or_create_new_datafile_record(odf)
        if not ndf: return False
        ndf.dataset = nds
        ndf.supersedes = odf
        ndf.qc_passed = True
        ndf.save()

    if not len(nds.datafile_set.all()) == len(ods.datafile_set.all()):
        return False

    return True


def check_can_fix(orig_ds):

    datafiles = orig_ds.datafile_set.all()
    for df in datafiles:
        qcerrors = df.qcerror_set.all().exclude(error_msg__startswith='ERROR (4): Axis attribute')
        for e in qcerrors:
            if e.check_type in ['QCPlot', 'LATEST', 'TIME-SERIES', 'TEMPORAL']:
                open(os.path.join(DATASETS_WONT_FIX_DIR, orig_ds.dataset_id), 'a').close()
                return False

    return True

def check_dataset_files_are_fixed(orig_ds):

    """

    :param orig_ds:
    :return: BOOLEAN - fixed dataset
    """

    datafiles = orig_ds.datafile_set.all()

    for df in datafiles:
        files_dir = os.path.dirname(df.gws_path).replace('latest', 'files/{}'.format(NEW_VERSION_NO))
        datafile = os.path.join(files_dir, os.path.basename(df.gws_path))

        if os.path.isfile(datafile):
            qcerrors = df.qcerror_set.all().exclude(error_msg__startswith='ERROR (4): Axis attribute')

            if qcerrors:
                errors_to_fix = set()
                for e in qcerrors:
                    if e.check_type in ['QCPlot', 'LATEST', 'TIME-SERIES', 'TEMPORAL']:
                        open(os.path.join(DATASETS_WONT_FIX_DIR, orig_ds.dataset_id), 'a').close()
                        return False
                    else:
                        errors_to_fix.add(e.error_msg)


                fix_status = check_datafile_fixed(orig_ds, datafile)

                if fix_status == "ok":
                    continue

                if fix_status == "Not fixed":
                    if not errors_to_fix:
                        symlinked = replace_file_with_symlink(orig_ds, datafile)
                        if not symlinked:
                            return False
                    else:
                        fixed_file = fix_datafile(orig_ds, datafile, list(errors_to_fix))
                        if fixed_file:
                            new_fix_status = check_datafile_fixed(orig_ds, datafile)

                            if not new_fix_status == 'ok':
                                write_error_log(orig_ds, 'FAILED to fix file {}'.format(datafile))
                                return False
                        else:
                            return False


                if fix_status == "Wrong fix":
                    symlinked = replace_file_with_symlink(orig_ds, datafile)
                    if not symlinked:
                        return False

        else:
            write_error_log(orig_ds, "No datafile to fix {}".format(datafile))
            return False

    return True


def fix_datafile(orig_ds, datafile, errors_to_fix):

    print "TRYING TO FIX file ", datafile

    qcFixer = QCerror_fixer()

    try:
        for error in errors_to_fix:
            qcFixer.qc_fix_wrapper(datafile, error)

        qcFixer.ncatted_common_updates(datafile, errors_to_fix)

    except:
        write_error_log(orig_ds, "FAILED TO FIX file: {}".format(datafile))
        return False

    return True



def replace_file_with_symlink(orig_ds, datafile):

    orig_version = orig_ds.version.strip('v')
    orig_file = datafile.replace(NEW_VERSION_NO, orig_version)
    if not os.path.isfile(orig_file):
        write_error_log(orig_ds, "original datafile missing {}".format(orig_file))
        return False
    print "symlinking {} {}".format(orig_file, datafile)
    os.symlink(orig_file, datafile)
    return True


def make_new_version_dir(orig_ds):

    orig_version = orig_ds.version
    datafiles = orig_ds.datafile_set.all()
    for df in datafiles:
        files_dir = os.path.dirname(df.gws_path).replace('latest', 'files/{}'.format(NEW_VERSION_NO))

        if not os.path.exists(files_dir):
            os.makedirs(files_dir)

        dst_file = os.path.join(files_dir, os.path.basename(df.gws_path))
        src_file = dst_file.replace(NEW_VERSION_NO, orig_version.strip('v'))

        qcerrors = df.qcerror_set.exclude(error_msg__startswith='ERROR (4): Axis attribute')

        if qcerrors:
            if not os.path.isfile(dst_file):
                print "    COPYING {} {}".format(src_file, dst_file)
                shutil.copyfile(src_file, dst_file)

        else:
            if os.path.isfile(dst_file):
                print "    remove file {}".format(dst_file)
                # Delete any datafile record also
                os.remove(dst_file)
            if not os.path.islink(dst_file):
                print "    SYMLINKING {} {}".format(src_file, dst_file)
                os.symlink(src_file, dst_file)

    orig_files = os.listdir(os.path.dirname(src_file))
    new_files = os.listdir(os.path.dirname(dst_file))

    if not len(orig_files) == len(new_files):
        if len(new_files) == 0:
            write_error_log(orig_ds, "All files pass QC")
            return False
        else:
            write_error_log(orig_ds, "Mismatch in number of files in dataset")
            return False

    return True


def write_ds_id_to_publish(orig_ds):

    id = orig_ds.dataset_id
    new_id = Dataset.objects.filter(dataset_id__icontains='.'.join(id.split('.')[:-1]), version=NEW_VERSION).first()
    if not new_id:
        write_error_log(orig_ds, "no new dataset id")
        return False

    with open(PUBLISH_LOG, 'a+') as w:
        w.writelines(["{}\n".format(new_id)])

    return True





def main_fix_ingest(id):

    # Generate dataset id from path

    ds = Dataset.objects.filter(dataset_id__icontains=id)

    if len(ds) == 0:
        write_error_log(id, 'No datasets found')
        return False, id, 'No datasets found'

    elif len(ds) == 1:
        if ds.first().version == NEW_VERSION:
            return False, ds, 'No original dataset record found'
        else:
            orig_ds = ds.first()

    elif len(ds) == 2:
        orig_ds = ds.exclude(version=NEW_VERSION).first()

    else:
        return False, ds, 'Too many dataset records found'
    print orig_ds

    print "can_fix"
    can_fix = check_can_fix(orig_ds)
    if not can_fix:
        return False, orig_ds, "Dataset has outstanding QC errors, currently not fixable"
    print can_fix

    print "all_files_in_new_files_dir"
    all_files_in_new_files_dir = make_new_version_dir(orig_ds)
    if not all_files_in_new_files_dir:
        return False, orig_ds, "FAIL : not all files in new version directory"
    print all_files_in_new_files_dir

    print "dataset_files_are_fixed"
    dataset_files_are_fixed = check_dataset_files_are_fixed(orig_ds)
    if not dataset_files_are_fixed:
        return False, orig_ds, "FAIL, datasets is not qc'd"
    print dataset_files_are_fixed

    print "database_records_ok"
    database_records_ok = update_database_records(orig_ds)
    if not database_records_ok:
        return False, orig_ds, "FAIL database records not updated ok"
    print database_records_ok

    print 'new_version_dir_created'
    new_version_dir_created = create_new_version(orig_ds)
    if not new_version_dir_created:
        return False, orig_ds, "FAILED TO CREATE NEW VERSION DIR"
    print new_version_dir_created

    print 'dataset_in_qc_dir'
    dataset_in_qc_dir = move_directory_to_qcd_dir(orig_ds)
    if not dataset_in_qc_dir:
        return False, orig_ds, "failed to move dataset"
    print dataset_in_qc_dir

    print 'write_ds_id_to_publish'
    publish_log = write_ds_id_to_publish(orig_ds)
    if not publish_log:
        return False, orig_ds, "Failed to write publish log"
    print publish_log

    return True, orig_ds, None

if __name__ == "__main__":

    # dataset_ids = os.listdir(DATASETS_TO_FIX_DIR)
    # for dsid in dataset_ids[:1]:

    dsid = sys.argv[1]

    to_fix_file = os.path.join(DATASETS_TO_FIX_DIR, dsid)
    in_progress_file = os.path.join(DATASETS_IN_PROGRESS_DIR, dsid)

    shutil.move(to_fix_file, in_progress_file)

    status_ok, ds, error_msg = main_fix_ingest(dsid)
    dsid = ds.dataset_id

    if not status_ok:
        wont_fix = os.listdir(DATASETS_WONT_FIX_DIR)
        if dsid in wont_fix:
            os.remove(in_progress_file)

        else:
            shutil.move(in_progress_file, os.path.join(DATASETS_FAILED_TO_COMPLETE_DIR, dsid))
            write_error_log(dsid, error_msg)

    else:
        shutil.move(in_progress_file, os.path.join(DATASETS_DONE_PUBLISH_DIR, dsid))



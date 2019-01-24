
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
PUBLISH_LOG = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/to_publish_to_esgf_2019-01-24.log'
ERROR_LOG = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/logfiles/fix_and_ingest_fixed_files_error_log.log'
DIRS_TO_FIX = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/datasets_to_fix_2019-01-24.log'
NEW_VERSION_NO = '20181201'
NEW_VERSION = 'v20181201'

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


def check_new_dataset_is_complete(ods, nds):

    orig_files = ods.datafile_set.all()
    orig_files_dir = os.path.dirname(orig_files.first().gws_path)
    orig_files_dir = os.path.join(orig_files_dir.rstrip('latest'), 'files', ods.version.strip('v'))
    n_orig_files = len(os.listdir(orig_files_dir))
    fixed_files = nds.datafile_set.all()
    fixed_files_dir = os.path.dirname(fixed_files.first().gws_path)
    fixed_files_dir = os.path.join(fixed_files_dir.rstrip('latest'), 'files', nds.version.strip('v'))
    n_fixed_files = len(os.listdir(fixed_files_dir))

    if not n_orig_files == len(orig_files):
        print("ERROR: Original dataset record and number of files on system are inconsistent {} {}".format(ods, orig_files_dir))

    if not n_fixed_files == len(fixed_files):
        print("ERROR: Fixed dataset record and number of files on system are inconsistent {} {}".format(nds, fixed_files_dir))

    for ofile in orig_files:
        new_file_version = os.path.join(fixed_files_dir, os.path.basename(ofile.gws_path))
        exists_new_file_version = os.path.exists(new_file_version)

        if exists_new_file_version:
            pass
            # print "exists {}".format(ofile)
            # ndf = DataFile.objects.filter(gws_path__icontains=new_file_version.replace('files/20181201', 'latest'), dataset__version='v20181201')
            # ndf = ndf.first()
            # print ndf
            # print ndf.qc_passed
            # print ndf.qc_fixed
            # print ndf.dataset.version

            # if not ofile.qc_passed:
            #     print("ERROR: New file does not pass QC")

        else:
            errors = ofile.qcerror_set.all()
            if not errors:
                print("File has no errors make file a symlink in new version dir {}".format(new_file_version))
                ofile.qc_passed = True
                continue

            fixable_errors = []
            for e in errors:
                if e.check_type in ['QCPlot', 'LATEST', 'TIME-SERIES', 'TEMPORAL']:
                    fixable_errors.append('No')
                else:
                    fixable_errors.append('Yes')

            if 'No' in fixable_errors:
                print("DataFile not fixable {}".format(ofile))
                ofile.qc_passed = False
                ofile.dataset.qc_passed = False
                continue

            else:
                if e.error_msg.startswith('ERROR (4): Axis attribute'):
                    print("Make file a symlink in new version {}".format(new_file_version))


                else:
                    print ofile.qc_fixed
                    print("I WILL TRY TO FIX {}".format(ofile.gws_path))
                    # filefixer.fix_file(ofile.gws_path)
                    # check file fixed records and exists on filesystem
                    fixed_df = DataFile.objects.filter(gws_path__icontains=new_file_version.replace('files/20181201', 'latest'), dataset__version='v20181201')
                    print fixed_df

        # Amend dataset records

        # fixed_df.qc_fixed = True
        # fixed_df.qc_passed = True
        # fixed_ds.dataset = nds
        # fixed_df.save()

        # nds.supersedes = ods
        # nds.qc_passed = True
        # nds.save()

    asdfasd

    if not len(os.listdir(fixed_files_dir)) == len(fixed_files):
        print("ERROR: Fixed dataset record and number of files on system are inconsistent {} {}".format(nds, fixed_files_dir))

    if not len(orig_files) == len(fixed_files):
        print("NOT ALL FILES IN FIXED DS {}".format(nds))



    else:
        for file in orig_files:
            for error in file.qcerror_set.all():
                if not error:
                    print("NEW FILE SHOULD BE SYMLINK {}".format(file))


    # version = fixed_ds.version
    # base_ds_id = '.'.join(fixed_ds.dataset_id.split('.')[:-1])
    # orig_ds = Dataset.objects.filter(dataset_id__startswith=base_ds_id).exclude(version=version).first()
    #
    # if not orig_ds:
    #     return False, []
    #
    # orig_files = orig_ds.datafile_set.all()
    # fixed_files = fixed_ds.datafile_set.all()
    #
    # if len(orig_files) == len(fixed_files):
    #     return True, []
    #
    # else:
    #     missing_files = set()
    #     orig_files = set(orig_files)
    #     fixed_files = set(fixed_files)
    #     missing_files = orig_files - fixed_files
    #
    #     missing_ok = check_missing_files_ok(missing_files, fixed_ds)
    #     if missing_ok:
    #         return True, missing_files
    #     else:
    #         return False, []


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


def check_datafile_errors(ods):

    datafiles = ods.datafile_set.all()

    for df in datafiles:
        qcerrors = df.qcerror_set.all()

        for error in qcerrors:
            if error.check_type not in ["CEDA-CC", "CF"]:
                return False

    return True


def ingest_fixed_dataset(ods, nds):

    print("Dataset {}".format(nds))
    # only ingest where errors from CEDA-CC and CF were attempted to be fixed
    print("Checking valid errors")
    valid_errors = check_datafile_errors(ods)
    if not valid_errors:
        print("NOT INGESTING AS INVALID ERRORS {}".format(ods))
        return

    print("Checking dataset is complete")

    dataset_is_complete = check_new_dataset_is_complete(ods, nds)

    # ds = Dataset.objects.filter(dataset_id=datasetID).first()
    #
    # dataset_is_complete, files_not_in_new_version = check_dataset_is_complete(ds)
    # if not dataset_is_complete:
    #     write_error_log(ds.dataset_id, "DATASET NOT COMPLETE")
    #     return
    # if dataset_is_complete:
    #     print files_not_in_new_version
    #     print len(files_not_in_new_version)
    # stopa
    # print "dataset_is_complete", dataset_is_complete
    #
    #
    # moved = move_completed_dataset(ds)
    # if not moved:
    #     write_error_log(ds.dataset_id, "MOVE FILES FAILED")
    #     return
    #
    # print "moved", moved
    #
    # df_records_updated = update_datafile_records(ds)
    # if not df_records_updated:
    #     write_error_log(ds.dataset_id, "DATABASE RECORDS NOT UPDATED")
    #     return
    #
    # print "df_records_updated", df_records_updated

    # write_dataset_id_for_publication(ds)




def convert_path_to_dataset_id(dir):

    inst, model, exp, frq, realm, table, ensemble, var = dir.split('/')[7:-2]
    id = '.'.join([inst, model, exp, frq, realm, table, ensemble, var])
    return id


def get_dataset_versions(datasetID):

    datasets = Dataset.objects.filter(dataset_id__icontains=datasetID)
    orig_ds = None
    new_ds = None

    for ds in datasets:
        if ds.version == 'v20181201':
            new_ds = ds
        else:
            orig_ds = ds

    return orig_ds, new_ds


def check_datafile_fixed(datafile):

    ncfile = nc.Dataset(datafile)
    try:
        cp4cds_glb_att = getattr(ncfile, 'cp4cds_update_info')
    except(AttributeError):
        write_error_log(orig_ds, "ERROR Datafile has not been corrected {}".format(df))
        return "Not fixed"

    if "  \n" in cp4cds_glb_att:
        write_error_log(orig_ds, "ERROR Datafile has been corrected with no errors {}".format(df))
        return "Wrong fix"

    return "ok"

def check_dataset_files_are_fixed(orig_ds):

    """

    :param orig_ds:
    :return: BOOLEAN - fixed dataset
    """

    datafiles = orig_ds.datafile_set.all()
    for df in datafiles:
        print df
        files_dir = os.path.dirname(df.gws_path).replace('latest', 'files/{}'.format(NEW_VERSION_NO))
        datafile = os.path.join(files_dir, os.path.basename(df.gws_path))
        if os.path.isfile(datafile):
            qcerrors = df.qcerror_set.all().exclude(error_msg__startswith='ERROR (4): Axis attribute')
            print qcerrors

            if qcerrors:
                errors_to_fix = set()
                for e in qcerrors:
                    if e.check_type in ['QCPlot', 'LATEST', 'TIME-SERIES', 'TEMPORAL']:
                        return False
                    else:
                        errors_to_fix.add(e.error_msg)


                fix_status = check_datafile_fixed(datafile)

                if fix_status == "Not fixed":
                    if not errors_to_fix:
                        symlinked = replace_file_with_symlink(orig_ds, datafile)
                        if not symlinked:
                            return False
                    else:
                        fixed_file = fix_datafile(orig_ds, datafile, list(errors_to_fix))
                        if fixed_file:
                            new_fix_status = check_datafile_fixed(datafile)

                            if not new_fix_status == 'ok':
                                write_error_log(orig_ds, 'FAILED to fix file {}'.format(datafile))
                                return False

                if fix_status == "Wrong fix":
                    symlinked = replace_file_with_symlink(orig_ds, datafile)
                    if not symlinked:
                        return False

        else:
            write_error_log(orig_ds, "No datafile to fix {}".format(datafile))
            return False

    return True


def replace_file_with_symlink(orig_ds, datafile):

    orig_version = orig_ds.version.strip('v')
    orig_file = datafile.replace(NEW_VERSION_NO, orig_version)
    if not os.path.isfile(orig_file):
        write_error_log(orig_ds, "original datafile missing {}".format(orig_file))
        return False
    print "sym linking {} {}".format(orig_file, datafile)
    os.symlink(orig_file, datafile)
    return True

def fix_datafile(datafile, errors_to_fix):
    qcFixer = utils.QCerror_fixer()
    try:
        for error in errors_to_fix:
            qcFixer.qc_fix_wrapper(new_file, error)

        qcFixer.ncatted_common_updates(new_file, errors_to_fix)

    except:
        write_error_log(orig_ds, "FAILED TO FIX file: {}".format(datafile))
        return False

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

        qcerrors = df.qcerror_set.all().exclude(error_msg__startswith='ERROR (4): Axis attribute')

        if qcerrors:
            if not os.path.isfile(dst_file):
                print "COPYING {} {}".format(src_file, dst_file)
                # shutil.copyfile(src_file, dst_file)

        else:
            if os.path.isfile(dst_file):
                print "remove file {}".format(dst_file)
                # Delete any datafile record also
                # os.remove(dst_file)
            if not os.path.islink(dst_file):
                print "SYMLINKING {} {}".format(src_file, dst_file)
                # os.symlink(src_file, dst_file)

    orig_files = os.listdir(os.path.dirname(src_file))
    new_files = os.listdir(os.path.dirname(src_file))

    if not len(orig_files) == len(new_files):
        write_error_log(orig_ds, "Mismatch in number of files in dataset")
        return False

    return True

if __name__ == "__main__":
    skip = [
        'CMIP5.output1.ICHEC.EC-EARTH.rcp45.mon.atmos.Amon.r6i1p1.uas',
        # MISSING FILES FROM ARCHIVE - restructure.py
        # 'CMIP5.output1.CMCC.CMCC-CMS.piControl.mon.seaIce.OImon.r1i1p1.sim.v20181201',
        # Dataset not complete
        # ERROR IN THE DATASET RECORD 2018 ok but time errors linked to original record
    ]

    # with open(DIRS_TO_FIX) as r:
    #     dirs = r.readlines()
    #
    # for d in dirs[:1]:
    for d in ['/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/output1/CSIRO-QCCCE/CSIRO-Mk3-6-0/rcp45/mon/atmos/Amon/r1i1p1/rsut/latest',
                #'/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/output1/CNRM-CERFACS/CNRM-CM5/rcp85/mon/atmos/Amon/r6i1p1/tasmin/latest',
                #'/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/output1/MOHC/HadCM3/historical/mon/ocean/Omon/r7i1p1/sos/latest',
                #'/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/output1/MOHC/HadGEM2-CC/rcp85/day/atmos/day/r1i1p1/prsn/latest',
              ]:

        dir = d.strip()
        id = '.'.join(dir.split('/')[7:-1])+'.'
        ds = Dataset.objects.filter(dataset_id__icontains=id)
        print ds
        if len(ds) == 0:
            write_error_log(id, 'No datasets found')
            continue
        if len(ds) == 1:
            if ds.first().version == NEW_VERSION:
                write_error_log(ds.first(), 'No original dataset record')
                continue
            else:
                orig_ds = ds.first()
                continue
        if len(ds) == 2:
            orig_ds = ds.exclude(version=NEW_VERSION).first()

        all_files_in_new_files_dir = make_new_version_dir(orig_ds)
        print "all_files_in_new_files_dir",all_files_in_new_files_dir
        dataset_files_are_fixed = check_dataset_files_are_fixed(orig_ds)
        print "dataset_files_are_fixed",dataset_files_are_fixed
        # Update the datafile and dataset records at appropriate place
        # add in new version symlink
        # move directory to qcd dir
        # write ds id to publish




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

    # ingest_dataset_list = "../ancil_files/files2018_dirs_to_ingest_2019-01-23.log"
    # with open(ingest_dataset_list, 'r') as r:
    #     ingest_list = r.readlines()

    # print ingest_list[:1]
    # for /dir in ingest_list:
        # for dir in [#'/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/output1/CSIRO-QCCCE/CSIRO-Mk3-6-0/rcp45/mon/atmos/Amon/r1i1p1/rsut/files/20181201',
        #             #'/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/output1/CNRM-CERFACS/CNRM-CM5/rcp85/mon/atmos/Amon/r6i1p1/tasmin/files/20181201',
        #             #'/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/output1/MOHC/HadCM3/historical/mon/ocean/Omon/r7i1p1/sos/files/20181201',
        #             '/group_workspaces/jasmin2/cp4cds1/data/cmip5_raw/output1/MOHC/HadGEM2-CC/rcp85/day/atmos/day/r1i1p1/prsn/files/20181201',]:

        # dir = dir.strip()
        #
        # datasetID = convert_path_to_dataset_id(dir)
        # if datasetID in skip:
        #     continue
        #
        # orig_ds, qcd_ds = get_dataset_versions(datasetID)
        #
        # if not orig_ds:
        #     print("MISSING : original dataset record {} {}".format(dir, datasetID))
        #     continue
        # if not qcd_ds:
        #     print("MISSING : qcd dataset record {} {}".format(dir, datasetID))
        #     with open('missing_qcd_dataset_record.log', 'a+') as w:
        #         w.writelines(["{} {}\n".format(datasetID, dir)])
        #     # errors = set()
        #     # dfs = orig_ds.datafile_set.all()
        #     # for df in dfs:
        #     #     qcerrs = df.qcerror_set.all()
        #     #
        #     #     for e in qcerrs:
        #     #         if not e.check_type in ['CEDA-CC', 'CF']:
        #     #             print "FAIL"
        #     #         errors.add(e.error_msg)
        #     # print errors
        #     # continue
        #
        #
        # # ingest_fixed_dataset(orig_ds, qcd_ds)
        #
        #



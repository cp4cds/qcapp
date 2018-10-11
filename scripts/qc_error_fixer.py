
from setup_django import *
import sys
import re
import shutil
from settings import *
from utils import *
import run_quality_control as run_qc


def check_error_types(ds, error):

    if error.check_type in ['QCPlot', 'LATEST', 'TIME-SERIES', 'TEMPORAL']:
        return
    else:
        if err.check_type == 'CF':
            error_messages.append(err.error_msg)
        if err.check_type == 'CEDA-CC':
            error_messages.append(err.error_msg)


def copy_file_to_new_files_version_dir(df, new_version_no):

    # Copy the file to files version dir so that inplace modifications can be made
    new_dir = os.path.dirname(df.gws_path).strip('latest') + "files/{}".format(new_version_no)
    new_file = os.path.join(new_dir, os.path.basename(df.gws_path))
    oversion = os.readlink(os.path.dirname(df.gws_path))
    if not os.path.isdir(new_dir):
        os.makedirs(new_dir)
    if not os.path.exists(new_file):
        shutil.copy(df.gws_path.replace('latest', oversion), new_file)

    return new_file


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

    return new_df

def get_or_create_new_dataset_record(ds, new_version_no):
    new_drs_version = '.'.join(ds.dataset_id.split('.')[:-1]) + '.v' + new_version_no
    nds_query = Dataset.objects.filter(dataset_id=new_drs_version)

    if len(nds_query) == 0:
        new_ds = ds
        new_ds.pk = None
        new_ds.version = 'v{}'.format(new_version_no)
        new_ds.save()

    else:
        new_ds = nds_query.first()

    # Attach all datafile records to new dataset
    for df in ds.datafile_set.all():
        ndf = get_or_create_new_datafile_record(df, new_ds, new_version_no)

    return new_ds


def qc_file_fix(error_messages, new_file):

    qcfix = QCerror_fixer()
    for error in error_messages:
        qcfix.qc_fix_wrapper(new_file, error)
    qcfix._ncatted_common_updates(new_file, error_messages)


def check_errors_can_fix(qc_errors):

    error_messages = set()
    for e in qc_errors:
        if e.check_type in ['QCPlot', 'LATEST', 'TIME-SERIES', 'TEMPORAL']:
            can_fix = False
            return can_fix, list(error_messages)
        else:
            can_fix = True
            if e.check_type == 'CF':
                error_messages.add(e.error_msg)
            if e.check_type == 'CEDA-CC':
                error_messages.add(e.error_msg)

    return can_fix, list(error_messages)



def create_new_filesystem_version(ds, version_no):

    ds_basepath = os.path.dirname(ds.datafile_set.first().gws_path).strip('latest')
    # check new files dir exists
    new_files_dir = os.path.join(ds_basepath, 'files', version_no)
    assert(new_files_dir)
    # assert(len(os.listdir(new_files_dir)) == len(os.listdir(os.path.dirname(ds.datafile_set.first().gws_path))))

    os.chdir(ds_basepath)
    new_version_dir = 'v'+version_no
    if not os.path.exists(new_version_dir): os.makedirs(new_version_dir)
    os.chdir(new_version_dir)
    for file in os.listdir(new_files_dir):
        if not file.endswith('.nc'): os.remove(os.path.join(new_files_dir, file))
        if not os.path.exists(os.path.join(ds_basepath, 'v{}'.format(version_no), file)):
            os.symlink(os.path.join('..', 'files', version_no, file), file)

    os.chdir('../')
    latest_link = 'latest'
    os.remove(latest_link)
    os.symlink(new_version_dir, latest_link)


def check_all_files_exist_in_new_version(orig_ds, new_version_no):
    """

    :return:
    """
    ds_basepath = os.path.dirname(orig_ds.datafile_set.first().gws_path).strip('latest')
    orig_version = orig_ds.version
    orig_files = os.listdir(os.path.join(ds_basepath, orig_version))
    new_files = os.listdir(os.path.join(ds_basepath, 'v{}'.format(new_version_no)))

    if len(orig_files) == len(new_files):
        print "    All psuedo archive files present at {}".format(os.path.join(ds_basepath, 'v{}'.format(new_version_no)))

    else:
        for file in orig_files:
            file_passed_qc = check_file_passed_qc(file)
            if not file_passed_qc:
                return 
            else:
                newfile = os.path.join(ds_basepath, 'v{}'.format(new_version_no), file)
                if not os.path.exists(newfile):
                    ofile = os.path.join(ds_basepath, 'files', orig_version.lstrip('v'), file)
                    os.symlink(ofile, newfile)
            

def check_file_passed_qc(file):

    df = DataFile.objects.filter(ncfile=file).first()
    if df.qc_passed:
        return True
    else:
        print("    FILE FAILED QC {}".format(df.gws_path))
        return False

def write_datasetid_to_log(esgf_log_file, nds):

    datasetid = "{}\n".format(nds.dataset_id)
    with open(esgf_log_file, "r+") as file:
        for line in file:
            if nds.dataset_id in line:
                break
        else:  # not found, we are at the eof
            file.write(datasetid)  # append missing data


def fix_errors(drs):

    new_version_no = '20181001'
    ds = Dataset.objects.filter(dataset_id=drs).first()
    if ds.version == "v{}".format(new_version_no):
        return

    print("Checking Dataset {}".format(ds))
    datafiles = ds.datafile_set.all()
    nds = None

    for df in datafiles:
        print("    Datafile {}".format(df))

        qc_errors = df.qcerror_set.all()

        # No QC errors for this file
        if not qc_errors:
            df.qc_passed = True
            df.save()

        else:
            can_fix, error_messages = check_errors_can_fix(qc_errors)

            # If can't fix
            if not can_fix:
                print("    FATAL QC")
                df.qc_passed = False
                df.save()
                ds.qc_passed = False
                ds.save()
                return

            # QC errors can be fixed
            else:
                print("    CAN FIX QC")
                nds = get_or_create_new_dataset_record(ds, new_version_no)
                nds.qc_passed = True
                nds.save()
                ndf = get_or_create_new_datafile_record(df, nds, new_version_no)

                # Copy the file to a new versions dir to perform QC-fix
                new_file = copy_file_to_new_files_version_dir(ndf, new_version_no)
                # Fix the file, via in-place modifications
                qc_file_fix(error_messages, new_file)

                qcdir = get_and_make_logdir(ndf, force_version=new_version_no)

                """
                RUN QC
                CEDA-CC writes output files - chdir to avoid them on psuedo-archive. 
                """
                os.chdir('/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev')
                run_qc.run_ceda_cc(new_file, qcdir)
                run_qc.parse_ceda_cc(ndf, qcdir)
                run_qc.run_cfchecker(new_file, qcdir)
                run_qc.parse_cf_checker(ndf, qcdir)

                # Check that there no errors left
                if len(ndf.qcerror_set.all()) != 0:
                    ndf.qc_passed = False
                    ndf.save()
                    nds.qc_passed = False
                    nds.save()
                    return
                else:
                    ndf.qc_passed = True
                    ndf.save()

    if nds.qc_passed:
        create_new_filesystem_version(nds, new_version_no)
        check_all_files_exist_in_new_version(ds, new_version_no)
        nds.supersedes = ds
        nds.save()
        esgf_log_file = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/datasets_to_publish_to_esgf.log"
        write_datasetid_to_log(esgf_log_file, nds)
    else:
        print("Dataset fails {}".format(ds))

if __name__ == "__main__":

    drs = sys.argv[1]    
    fix_errors(drs)
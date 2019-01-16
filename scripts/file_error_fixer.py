
from setup_django import *
from settings import *
import os
import utils
import sys
import shutil

FIX_FAIL_LOG = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/logfiles/failed_to_fix.log'


def write_fix_fail_log(file, msg):
    print msg
    # with open(FIX_FAIL_LOG, 'a+') as w:
    #     w.writelines(["{} {}\n".format(msg, file)])


def make_new_version_file(df, new_version_no):

    # Copy the file to files version dir so that inplace modifications can be made
    new_dir = os.path.dirname(df.gws_path).strip('latest') + "files/{}".format(new_version_no)
    new_file = os.path.join(new_dir, os.path.basename(df.gws_path))
    orig_version = os.readlink(os.path.dirname(df.gws_path))

    if not os.path.isdir(new_dir):
        os.makedirs(new_dir)

    if not os.path.exists(new_file):
        shutil.copy(df.gws_path.replace('latest', orig_version), new_file)

    return new_file


def fix_file(file):

    new_version_no = '20181201'
    df = DataFile.objects.filter(gws_path__startswith=file, supersedes__isnull=True).first()
    print df.gws_path
    print os.readlink(df.gws_path)

    try:
        new_file = make_new_version_file(df, new_version_no)
    except:
        write_fix_fail_log(file, 'NEW FILE ERROR')
        return

    qcFixer = utils.QCerror_fixer()

    errs = df.qcerror_set.all()
    errors_to_fix = set()

    for e in errs:
        if e.error_msg.startswith('ERROR (4): Axis attribute '):
            continue
        errors_to_fix.add(e.error_msg)

    try:
        for error in list(errors_to_fix):
            qcFixer.qc_fix_wrapper(new_file, error)

        qcFixer.ncatted_common_updates(new_file, list(errors_to_fix))

    except:
        write_fix_fail_log(file, 'COMMON UPDATES ERROR')
        return


    try:
        new_ds = get_or_create_new_dataset_record(df.dataset, new_version_no)
    except:
        write_fix_fail_log(file, 'NEW DATASET RECORD ERROR')
        return

    if new_ds:
        try:
            new_df = get_or_create_new_datafile_record(df, new_ds, new_version_no)
        except:
            write_fix_fail_log(file, 'NEW DATAFILE RECORD ERROR')
            return

    # create new version directory and symlinks when the dataset is complete.


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

    # # Attach all datafile records to new dataset
    # for df in ds.datafile_set.all():
    #     ndf = get_or_create_new_datafile_record(df, new_ds, new_version_no)



if __name__ == "__main__":

    filename = sys.argv[1]
    fix_file(filename)



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


def create_datafile_basepath(df):

    var_base_path = os.path.dirname(df.gws_path).strip('latest').rstrip('/').replace(QC_PASSED_BASE, QC_FAILED_BASE)
    if not os.path.isdir(var_base_path):
        os.makedirs(var_base_path)

    return var_base_path


def create_files_directory(var_path, version):

    files_version_dir = os.path.join(var_path, 'files', version)
    if not os.path.isdir(files_version_dir):
        os.makedirs(files_version_dir)

    return files_version_dir


def create_version_directory(var_path, version):

    version_dir = os.path.join(var_path, version)
    if not os.path.isdir(version_dir):
        os.makedirs(version_dir)
    print version_dir
    return version_dir


def symlink_version_files(files_version_dir, version):

    for file in os.listdir(files_version_dir):
        print file
        print os.path.join('../../', version, file)
        os.symlink(file, os.path.join('../../', version, file))
        print "LINKING", file, os.path.join('../../', version, file)


def create_latest_directory(var_base_path, version):

    print "Chdir", var_base_path
    os.chdir(var_base_path)
    print "link", 'latest', version
    os.symlink('latest', version)

    return os.path.join(var_base_path, 'latest')


def move_failed_datasets_data(datasets):


    for ds in datasets[:1]:
        print ds
        assert(not ds.qc_passed)
        datafiles = ds.datafile_set.all()
        df1 = datafiles.first()
        src = os.path.dirname(df1.gws_path).strip('latest').rstrip('/')
        dst = os.path.dirname(df1.gws_path).strip('latest').rstrip('/').replace(QC_PASSED_BASE, QC_FAILED_BASE)

        print "COPY ", src, dst

        shutil.copytree(src, dst, symlinks=True)

        # for df in datafiles:
        #     update_database(df, latest_dir)


        print "REMOVE ", src

        # version_no = ds.version.lstrip('v')
        # print version_no
        # datafiles = ds.datafile_set.all()
        # print "n_datafiles: ", len(datafiles)
        # var_base_path = create_datafile_basepath(datafiles.first())
        # print var_base_path
        # files_version_dir = create_files_directory(var_base_path, version_no)
        # print files_version_dir
        # create_version_directory(var_base_path, ds.version)
        #
        # for df in datafiles:
        #     print "MOVING ", df.gws_path, files_version_dir
        #     shutil.move(df.gws_path, files_version_dir)
        #     print "remove", df.gws_path
        #     # os.remove(df.gws_path)
        #
        # print "REMOVE DIRS", var_base_path
        # # shutil.rmtree(var_base_path)
        #
        # symlink_version_files(files_version_dir, ds.version)
        #
        # latest_dir = create_latest_directory(var_base_path, ds.version)
        # print latest_dir
        #


def update_database(df, latest_dir):

    new_gws_path = os.path.join(latest_dir, df.ncfile)
    assert (os.path.islink(new_gws_path))
    print "NEW GWS PATH",new_gws_path
    # df.gws_path = new_gws_path



def main():

    if FIX_DB_QC_FLAG:
        fix_db_status()

    failed_datasets = Dataset.objects.exclude(qc_passed=True)

    move_failed_datasets_data(failed_datasets)


if __name__ == "__main__":

    main()
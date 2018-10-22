
from setup_django import *
import sys
import re
import shutil
import json
import subprocess
from settings import *
from utils import *

import run_quality_control as run_qc
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings()


def get_latest_version(datasets):

    # dss = set()
    # for dataset in datasets:
    #     print dataset
    print datasets
    superseders = datasets.filter(supersedes__isnull=False)
    superseded_by = set()
    for ds in superseders:
        orig_version = ds.supersedes.version
        orig_drs = '.'.join(ds.dataset_id.split('.')[:-1]) + '.{}'.format(orig_version)
        orig_record = Dataset.objects.filter(dataset_id=orig_drs).first()
        superseded_by.add(orig_record)

    dss = set(datasets) - set(superseders) - superseded_by
    print dss
        # for i in s: dss.add(s)

    for ds in list(dss):
        datafiles = ds.datafile_set.all()

        for df in datafiles:
            errors = df.qcerror_set.all()

            for e in errors:
                if 'VERSION' in e.error_type:
                    esgf_query = e.error_msg.split(' :: ')[-1].lstrip('ESGF query').strip()
                    # print("Getting a newer version {}, {}, {}".format(ds, e.error_type, e.error_msg))
                    get_newer_version(esgf_query)


def parse_json(url):
    resp = requests.get(url, verify=False)
    json_resp = resp.json()
    results = json_resp["response"]["docs"]
    return results


def get_newer_version(query_url):
    """
    :param url: 
    :return: 
    """
    json_results = parse_json(query_url)
    
    replica_status = {}
    for res in json_results:
        replica_status[res['data_node']] = res['replica']
    no_master_copies = sum(1 for replica in replica_status.values() if not replica)

    if no_master_copies == 1:
        file_url = get_file_url(json_results, replica_status)
        if file_url:
            wget_new_file(file_url)
        else:
            print("No file url {}".format(query_url))
    else:
        print("Master copy can not be identified {}".format(query_url))


def get_file_url(json_results, replica_status):

    file_url = ''
    for k, v in replica_status.iteritems():
        if not v:
            datanode = k


    for res in json_results:
        if res['data_node'] == datanode:
            try:
                file_url = res['url'][0].split('|')[0]
            except:
                print("Unable to get file url")

    return file_url
        

def wget_new_file(url):

    certificate = '/home/users/rpetrie/.globus/certificate-file'
    odir = '/group_workspaces/jasmin2/cp4cds1/data/new_version'
    ofile = os.path.join(odir, url.split('/')[-1])
    if not os.path.exists(ofile):
        try:
            cmd = ['wget', '-nv', '--certificate', certificate, '--no-check-certificate', url, '-O', ofile]
            subprocess.call(cmd)
            print("RETRIEVED file :: {}".format(ofile))
        except:
            print("FAILED to retrieve :: {}".format(url))


def check_error_types(ds, error):

    if error.check_type in ['QCPlot', 'LATEST', 'TIME-SERIES', 'TEMPORAL']:
        return
    else:
        if err.check_type == 'CF':
            error_messages.append(err.error_msg)
        if err.check_type == 'CEDA-CC':
            error_messages.append(err.error_msg)

def ingest_new_files():
    version_no = '20181001'
    arrivals_dir = '/group_workspaces/jasmin2/cp4cds1/data/new_version/'
    psuedo_archive = '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/'

    for file in os.listdir(arrivals_dir):
        var, table, model, expt, ens, time = file.split('_')
        inst = get_institute_from_model(model)
        freq = get_frequency_from_table(table)
        realm = get_realm_from_table(table)
        deposit_path_base = os.path.join(psuedo_archive, inst, model, expt, freq, realm, table, ens, var)
        deposit_path_files_version = os.path.join(deposit_path_base, 'files', version_no)
        if os.path.exists(deposit_path_files_version):
            print("ERROR path exists {}".format(deposit_path_files_version))
            pass
        else:

            print("MAKING DIR {}".format(deposit_path_files_version))
            # os.makedirs(deposit_path_files_version)


        src = os.path.join(arrivals_dir, file)
        print("COPY {} TO  {}".format(src, deposit_path_files_version))
        # shutil.copy(src, deposit_path_files_version)

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
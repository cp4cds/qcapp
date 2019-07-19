
from setup_django import *
import os
import sys
import re
import settings
import subprocess
import utils
import shutil

QCD_DIR = '/alpha/c3scmip5/'
RAW_DIR = '/cmip5_raw/'


def check_timeseries_errors():
    
    for ds in Dataset.objects.all():

    # ds = Dataset.objects.filter(variable='pr', frequency='day', model='IPSL-CM5A-LR', experiment='amip', ensemble='r2i1p1').first()
        dfs = ds.datafile_set.all()
        ts_error = list(ds.qcerror_set.filter(check_type='TIME-SERIES'))
        if ts_error:
            for df in dfs:
                if not re.search('cmip5_raw', df.gws_path):
                    print df




def convert_paths():
    
    with open('timeseries_error_list.log') as r:
        filepaths = [line.strip() for line in r]
    
    uniq_paths = set()
    for path in filepaths:
       uniq_paths.add('/'.join(path.split('/')[:-2]))

    for up in list(uniq_paths):
        with open('dir_errors.log', 'a+') as w:
            w.writelines(["{}\n".format(up)])


def move_timeseries_fail_datasets():

    with open('ds_to_be_retracted_esgf_2019-02-13.log') as r:
        dirs = [line.strip() for line in r]

    for src in dirs[1:]:
        dst = src.replace(QCD_DIR, RAW_DIR)
        if not os.path.isdir(dst):
            print "MOVE ", src, dst
            shutil.move(src, dst)
            update_database_records(src)
        else:
            print "ERROR dir exists ", dst
            print "ERROR rm src ", src
        

def update_database_records(path):
    
    model, exp, freq, realm, table, ens, var = path.split('/')[9:]
    ds = Dataset.objects.filter(model=model, experiment=exp, frequency=freq, realm=realm, cmor_table=table,
                                ensemble=ens, variable=var).exclude(version='v20181201').first()
    print ds
    ds.qc_passed = False
    ds.save()
    
    for df in ds.datafile_set.all():

        errors = df.qcerror_set.exclude(error_msg__icontains='ERROR (4): Axis attribute')
        if errors: df.qc_passed = False
        else: df.qc_passed = True

        df.gws_path = df.gws_path.replace(QCD_DIR, RAW_DIR)
        df.save()


def convert_dirs_to_datset_ids():

    with open('ds_to_be_retracted_esgf_2019-02-13.log') as r:
        dirs = [line.strip() for line in r]

    for dir in dirs:

        dir = dir.replace(QCD_DIR, RAW_DIR)
        df = DataFile.objects.filter(gws_path__icontains=dir).first()
        ds = df.dataset
        print ds.dataset_id.replace("CMIP5", "c3s-cmip5")
        

if __name__ == "__main__":
    
    # convert_paths()
    # check_timeseries_errors()
    # move_timeseries_fail_datasets()
    convert_dirs_to_datset_ids()
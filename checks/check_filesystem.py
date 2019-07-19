from setup_django import *
import os
import shutil


def write_file(fname, line):
    with open(fname, 'a+') as w:
        w.writelines(["{}\n".format(line)])


def read_file(fname):
    with open(fname) as r:
        list = [line.strip() for line in r]

    return list

def check_datasets_in_c3sdir(datasets):
    
    for id in datasets:

        if 'fx' in id:
            continue

        ds = Dataset.objects.filter(dataset_id=id.replace('c3s-cmip5', 'CMIP5')).first()
        if not ds:
            print "ERROR not ds", id
            continue

        for df in ds.datafile_set.all():
            if 'cmip5_raw' in df.gws_path:
                print "error", df, ds

def check_mapfiles(mf_name):
    
    mfs = read_file(mf_name)
    MF_GWS_ROOT = '/group_workspaces/jasmin2/cp4cds1/data/c3s-cmip5/.mapfiles'
    for mf in mfs:
        project, output, inst, model, exp, freq, realm, mapfile = mf.split('/')[5:]
        gws_mapfile = os.path.join(MF_GWS_ROOT, project, output, inst, model, exp, freq, realm, mapfile)
        if os.path.exists(gws_mapfile):
            print gws_mapfile


if __name__ == "__main__":


    # datasets = read_file('publish-to-thredds')
    # check_datasets_in_c3sdir(datasets)
    mf_name = 'mapfiles.log'
    check_mapfiles(mf_name)
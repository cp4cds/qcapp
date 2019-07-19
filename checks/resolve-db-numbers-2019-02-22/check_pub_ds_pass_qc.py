
from setup_django import *
import os
import shutil
import run_quality_control as qc


def gen_diff_list():
    for ds in in_psql:

        if ds in in_tds:
            write_file('in-tds-list', ds)

        elif 'fx' in ds:
            write_file('fx-list', ds)

        elif ds in qc_fixed:
            write_file('qc-fixed-list', ds)

        else:
            write_file('diff-list', ds)


def write_file(fname, line):

    with open(fname, 'a+') as w:
        w.writelines(["{}\n".format(line)])


def read_file(fname):
    
    with open(fname) as r:
        list = [line.strip() for line in r]
        
    return list

if __name__ == "__main__":

    publish = read_file('publish-to-thredds.old')
    fperrs = read_file('unpublish-from-psql')

    for ds in publish:
        if ds in fperrs:
            continue
        else:
            print ds

    # 
    # publish_list = read_file('in_psql_not_tds.log')
    # for id in publish_list:
    #     id = id.replace('c3s-cmip5', 'CMIP5')
    #     ds = Dataset.objects.filter(dataset_id__icontains=id).first()
    #     if not ds:
    #         continue
    #     print ds.dataset_id.replace('CMIP5', 'c3s-cmip5')
    #     # publish.append(ds.dataset_id.replace('CMIP5', 'c3s-cmip5'))
    # 

    # publish = read_file('publish-to-thredds')
    # inpsql = read_file('list-from-psql')
    #
    # for ds in publish:
    #     if ds not in inpsql:
    #         print ds

    # publish = []
    # publish_list = read_file('in_psql_not_tds.log')
    # for id in publish_list:
    #     id = id.replace('c3s-cmip5', 'CMIP5')
    #     ds = Dataset.objects.filter(dataset_id__icontains=id).first()
    #     if not ds:
    #         continue
    #     publish.append(ds.dataset_id.replace('CMIP5', 'c3s-cmip5'))
    #     # publish_set.add(ds.dataset_id.replace('CMIP5', 'c3s-cmip5'))
    #
    # fx_list = read_file('fx-list')
    # for d in fx_list:
    #     publish.append(d)
    #
    # qc_fixed_list = read_file('qc_fixed/valid_dataset_versions')
    # for d in qc_fixed_list:
    #     publish.append(d.replace('CMIP5', 'c3s-cmip5'))
    #     # qc_fixed_set.add(d.replace('CMIP5', 'c3s-cmip5'))
    #
    # # to_publish = publish_set | fx_set | qc_fixed_set
    # for ds in publish:
    #     print ds

    
    # diff_ds = read_file('diff-list')
    # datasets = read_file('check_if_ds_pass_qc.log')
    # qc_passed = []
    # for d in datasets:
    #     qc_passed.append(d.replace('CMIP5','c3s-cmip5') + ".")


    # ts_errors = read_file('timeseries_error_dataset_ids')
    # qc_fixed_ds = read_file('qc_fixed/valid_dataset_versions')
    # qc_fixed = []
    # for d in qc_fixed_ds:
    #     qc_fixed.append(d.replace('CMIP5', 'c3s-cmip5'))
    #
    # psql = read_file('list-from-psql')
    # in_psql = []
    # for i in psql:
    #     in_psql.append('.'.join(i.split('.')[:-1])+'.')
    #
    #
    # tds = read_file('list-from-thredds')
    # in_tds = []
    # for i in tds:
    #     in_tds.append('.'.join(i.split('.')[:-1])+'.')
    #


    # in_psql = read_file('in_psql_not_tds.log')
    # df_fails = read_file('datafile-path-error-unpublish.log')
    # df_fails_set = set()
    # for d in df_fails:
    #     df_fails_set.add(d)
    #     
    # fatals = read_file('fatal-dataset-not-exist-remove-from-psql.log')
    # fatals_set = set()
    # for d in fatals:
    #     fatals_set.add(d)
    # 
    # ts_fails = read_file('timeseries-fails-unpublish.log')
    # ts_fails_set = set()
    # for d in ts_fails:
    #     ts_fails_set.add(d)
    # 
    # all_fails = df_fails_set | fatals_set | ts_fails_set
    # 
    # for ds in ts_fails:
    #     if ds in df_fails:
    #         print "not in"

    # for ds in list(all_fails):
    #     print ds

    # for dsid in datasets:
    #     dsid = dsid.replace('c3s-cmip5', 'CMIP5')
    #     ds = Dataset.objects.filter(dataset_id__icontains=dsid).first()
    #
    #     # if not ds:
    #     #     print "not ds", ds
    #     #     write_file('fatals', dsid)
    #     #     continue
    #
    #     ds_errors = ds.qcerror_set.all()
    #     if not len(ds_errors) == 0:
    #         # write_file('timeseries-fails-unpublish.log', ds.dataset_id.replace('CMIP5', 'c3s-cmip5'))
    #         print dsid

        # dfs = ds.datafile_set.all()
        # for df in dfs:
        #     df_errors = df.qcerror_set.filter(error_msg='ERROR (4)')
        #
        #     df_fails = False
        #     if not len(df_errors) == 0:
        #         df_fails = True
        #         print "datafile fails ", ds.dataset_id.replace('CMIP5', 'c3s-cmip5')
                # write_file('datafile-fails-unpublish.log', ds.dataset_id.replace('CMIP5', 'c3s-cmip5'))


            # if 'cmip5_raw' in df.gws_path:
            #     df_fails = True
            #     break

        # if df_fails:
        #     write_file('datafile-path-error-unpublish.log', ds.dataset_id.replace('CMIP5', 'c3s-cmip5'))



    #     if not ds:
    #         write_file('fatal-dataset-not-exist.log', dsid)
    #         continue
    #
    #     ds_errors = ds.qcerror_set.all()
    #
    #     if not len(ds_errors) == 0:
    #         write_file('timeseries-fails.log', dsid)
    #         continue
    #
    #     for df in ds.datafile_set.all():
    #         df_errors = df.qcerror_set.all()
    #         df_fail = False
    #         if not len(df_errors) == 0:
    #             df_fail = True
    #             write_file('datafile-fails.log', dsid)
    #             continue
    #         if 'cmip5_raw' in df.gws_path:
    #             df_fail = True
    #             write_file('datafile-fails.log', dsid)
    #             continue
    #
    #     if df_fail:
    #         continue
    #
    #     write_file('dataset-passes-qc-publish.log', dsid)


    #     for df in ds.datafile_set.all():
    #         if "cmip5_raw" in df.gws_path:
    #             print path_error, ds

    # for ds in qc_fixed_ds:
    #     if ds.replace('CMIP5', 'c3s-cmip5') in ts_errors:
    #         print ds

    # datasets = read_file('datasets_in_psql_to_republish')
    # datasets = read_file('valid_dataset_versions')
    # 
    # 
    # for dsid in datasets:
    # 
    #     # if "fx" in dsid:
    #     #     print "found fx file"
    #     # 
    #     # if not "20181201" in dsid:
    #     #     print "not a fixed dataset"
    # 
    #     ds = Dataset.objects.filter(dataset_id=dsid).first()
    #     for df in ds.datafile_set.all():
    #         if "cmip5_raw" in df.gws_path:
    #             print path_error, ds
    # 
        # ds = Dataset.objects.filter(dataset_id__icontains='.'.join(dsid.split('.')[1:-1])).exclude(version='v20181201').first()
        # if not ds:
        #     continue
        # if ds in ts_errors:
        #     continue
        #
        # for df in ds.datafile_set.all():
        #     errors = df.qcerror_set.exclude(error_msg__icontains='ERROR (4)')
        #     if not len(errors) == 0:
        #         print "datafile errors",
        #         for e in errors:
        #             print e.error_msg
        #         with open('datasets_with_datafile_errors', 'a+') as w:
        #             w.writelines(["{}\n".format(dsid)])
        #         continue
        # ds_errors = ds.qcerror_set.all()
        #
        # if not len(ds_errors) == 0:
        #     print "dataset errors", ds, ds_errors, len(ds_errors)
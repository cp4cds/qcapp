
from setup_django import *
import os
import shutil


published_log = '../ancil_files/c3s-cmip5_datasets_in_psql.log'
dirsinqc = '../ancil_files/dirs_passed_qc.log'

def convert_path_to_id(path):

    version = os.readlink(path)


with open(published_log) as r:
    tmp = r.readlines()

published_ds = set()
for line in tmp:
  published_ds.add(line.strip())

print len(list(published_ds))

with open(dirsinqc) as r:
    t = r.readlines()

all_ds_to_publish = set()
for dir in t:
    dir = dir.strip()
    ins, model, exp, frq, realm, table, ens, var = dir.split('/')[8:-1]
    id = '.'.join(['c3s-cmip5', 'output1', ins, model, exp, frq, realm, table, ens, var])
    all_ds_to_publish.add(id)

print len(list(all_ds_to_publish))
system_ds_not_published = all_ds_to_publish - published_ds

print len(list(system_ds_not_published))

print list(system_ds_not_published)[0]

for ds in list(system_ds_not_published):
    if not "fx" in system_ds_not_published:
        with open('../ancil_files/to_publish_16_jan_2019/qc_passed_jan_24_2019.log', 'a+') as w:
            w.writelines(["{}\n".format(ds.replace('c3s-cmip5', 'CMIP5'))])


# with open('failed_datasets.log') as r:
#     ds_list = r.readlines()
#
# for id in ds_list:
#     id = id.strip()
#     ds = Dataset.objects.filter(dataset_id__icontains=id).first()
#     dfs = ds.datafile_set.all()
#     ds_dir = os.path.dirname(dfs.first().gws_path).replace('/latest','')
#
#     # if "raw" in ds_dir:
#     #     if not os.path.exists(ds_dir):
#     #         orig_dir = ds_dir.replace('cmip5_raw','alpha/c3scmip5')
#     #         orig_dir_exists = os.path.exists(orig_dir)
#     #         raw_dir = ds_dir
#     #         shutil.move(orig_dir,raw_dir)
#
#
#     if not 'raw' in ds_dir:
#
#         raw_dir = ds_dir.replace('alpha/c3scmip5', 'cmip5_raw')
#         print "move", ds_dir, raw_dir
#         shutil.move(ds_dir, raw_dir)
#
#         for df in dfs:
#             df.gws_path = df.gws_path.replace('alpha/c3scmip5', 'cmip5_raw')
#             df.save()
#
#     # if not os.path.exists(ds_dir):
#     #     print "Doesn't exist {}".format(ds_dir)
#     # print os.path.exists(ds_dir.replace('alpha/c3scmip5', 'cmip5_raw'))



    #
# published_log = '../ancil_files/c3s-cmip5_datasets_in_psql.log'
# dirsinqc = '../ancil_files/dirs_passed_qc.log'
#
# def convert_path_to_id(path):
#
#     version = os.readlink(path)
#
#
# with open(published_log) as r:
#     tmp = r.readlines()
#
# published_ds = set()
# for line in tmp:
#   published_ds.add(line.strip())
#
# print len(list(published_ds))
#
# with open(dirsinqc) as r:
#     t = r.readlines()
#
# all_ds_to_publish = set()
# for dir in t:
#     dir = dir.strip()
#     ins, model, exp, frq, realm, table, ens, var = dir.split('/')[8:-1]
#     id = '.'.join(['c3s-cmip5', 'output1', ins, model, exp, frq, realm, table, ens, var])
#     all_ds_to_publish.add(id)
#
# print len(list(all_ds_to_publish))
# system_ds_not_published = all_ds_to_publish - published_ds
#
# print len(list(system_ds_not_published))
#
# fx_ds = []
# data_ds = []
# for i in list(system_ds_not_published):
#     if "fx" in i:
#         fx_ds.append(i)
#     else:
#         data_ds.append(i)
#
# print len(fx_ds)
#
# print len(data_ds)
#
# failed_datasets = set()
#
# for id in data_ds:
#     id = id.replace('c3s-cmip5', 'CMIP5')
#     ds = Dataset.objects.filter(dataset_id__icontains=id).exclude(version='v20181201').first()
#     if not ds:
#         with open('missing_dataset_in_db.log', 'a+') as w:
#             w.writelines("{}\n".format(id))
#         continue
#     dfs = ds.datafile_set.all()
#
#     for df in dfs:
#         # if "raw" in df.gws_path:
#         #     if not os.path.exists(df.gws_path):
#         #         qc_path = df.gws_path.replace('cmip5_raw', 'alpha/c3scmip5')
#         #         if os.path.exists(qc_path):
#         #             df.gws_path=qc_path
#         #             df.save()
#         #         else:
#         #             print "ERROR  {}".format(df)
#         errs = df.qcerror_set.all()
#         if errs:
#             for e in errs:
#                 if e.check_type not in ['CEDA-CC', 'CF']:
#                     # print "Datafile fails qc {}".format(df)
#                     failed_datasets.add(ds)
#
# for fds in failed_datasets:
#     with open('failed_datasets.log', 'a+') as w:
#         w.writelines("{}\n".format(fds))
#



# '/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/MOHC/HadGEM2-ES/rcp45/mon/atmos/Amon/r1i1p1/tas/latest\n'
# 'c3s-cmip5.output1.MOHC.HadGEM2-ES.historical.day.atmos.day.r1i1p1.tas'


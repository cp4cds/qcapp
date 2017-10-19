#from django.test import TestCase

# Create your tests here.

import django, sys, os

django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg
import collections, os, timeit, datetime, sys
import requests, itertools
# IMPORT QC STUFF (only works in venv27)
from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import netCDF4
requests.packages.urllib3.disable_warnings()
import pdb
from django.shortcuts import render
from django.http import HttpResponse

from esgf_search_functions import *
from qcapp.models import *
from app_functions import *
import argparse
import commands


def endswith():
    files = [
             "/badc/cmip5/data/cmip5/output1/MOHC/HadCM3/historical/mon/atmos/Amon/r8i1p1/v20110905/tas/tas_Amon_HadCM3_historical_r8i1p1_195912-198411.nc4",
             "/badc/cmip5/data/cmip5/output1/MOHC/HadCM3/historical/mon/atmos/Amon/r8i1p1/v20110905/tas/tas_Amon_HadCM3_historical_r8i1p1_190912-193411.nc",
             "/badc/cmip5/data/cmip5/output1/MOHC/HadCM3/historical/mon/atmos/Amon/r8i1p1/v20110905/tas/tas_Amon_HadCM3_historical_r8i1p1_188412-190911.nc",
             "/badc/cmip5/data/cmip5/output1/MOHC/HadCM3/historical/mon/atmos/Amon/r8i1p1/v20110905/tas/tas_Amon_HadCM3_historical_r8i1p1_198412-200512.nc",
            ]

    for file in files:
        res = is_ceda_file(file)
        print res


def is_ceda_file(file):

    if not os.path.basename(file).endswith(".nc"):
        pass
    else:
        if os.path.isfile(file):
            result = True
        else:
            result = False

        return result

def check_latest_datasets():
    log_file = '/usr/local/cp4cds-app/qcapp/qcapp/latest-datafiles.log'
    log_file = '/usr/local/cp4cds-app/qcapp/qcapp/latest-datafiles-3.log'
    if not os.path.isfile(log_file):
        with open(log_file, 'w') as fw:
            fw.write('')

    node = "172.16.150.171"
    project = "CMIP5"
    latest = True
    distrib = True
    replica = False

    URL_TEMPLATE = 'https://%(node)s/esg-search/search?type=File&project=%(project)s&' \
                   'institute=%(institute)s&' \
                   'model=%(model)s&' \
                   'experiment=%(experiment)s&' \
                   'time_frequency=%(frequency)s&' \
                   'cmor_table=%(table)s&' \
                   'ensemble=%(ensemble)s&' \
                   'variable=%(variable)s&' \
                   'latest=%(latest)s&distrib=%(distrib)s&replica=%(replica)s&' \
                   'format=application%%2Fsolr%%2Bjson&limit=10000'
    
    
    for df in DataFile.objects.all():

        # institute = "NOAA-GFDL"
        # model = "GFDL-CM2.1"
        # experiment = "historical"
        # frequency = "mon"
        # realm = "atmos"
        # table = "Amon"
        # ensemble = "r7i1p1"
        # start_time = datetime.date(2001,1,1)
        # end_time = datetime.date(2005,12,31)
        # variable = "tas"

        institute = df.dataset.institute
        model = df.dataset.model
        experiment = df.dataset.experiment
        frequency = df.dataset.frequency
        table = df.dataset.cmor_table
        ensemble = df.dataset.ensemble
        #start_time, end_time = get_start_end_times(frequency, os.path.basename(df.archive_path))
        variable = df.variable

        # df = DataFile.objects.filter(dataset__institute=institute, dataset__model=model,
        #                              dataset__experiment=experiment, dataset__frequency=frequency, dataset__realm=realm,
        #                              dataset__cmor_table=table, dataset__ensemble=ensemble,
        #                              start_time=start_time, variable=variable)
        #
        # if df.count() == 1: df = df.first()

        url = URL_TEMPLATE % vars()
        resp = requests.get(url, verify=False)
        json = resp.json()
        try:
            for resp in range(len(json["response"]["docs"])):
                json_resp = json["response"]["docs"][resp]
                if json_resp["title"] == os.path.basename(df.archive_path):
                    id = json_resp["id"].strip()
                    checksum = json_resp["checksum"][0].strip()
                    checksum_type = json_resp["checksum_type"][0].strip()
                    datanode = id.split('|')[1]
                    dataset_id = id.split('|')[0]
                    version = dataset_id.split('.')[-3]
        except IndexError:
            with open(log_file, 'a') as fe:
                fe.write("NO URL RESPONSE: %s \n" % url)

        if checksum_type == "MD5":
            file_checksum = df.md5_checksum
        else:
            file_checksum = df.sha256_checksum

        if checksum == '' or df.sha256_checksum == '':
            with open(log_file, 'a') as fe:
                fe.write("MISSING CHECKSUMS: %s, %s, %s \n" % (id, variable, datanode))
        elif checksum == file_checksum:
            with open(log_file, 'a') as fe:
                fe.write("MATCH: %s, %s, %s \n" % (id, variable, datanode))
                fe.write("      LATEST CHECKSUM: %s \n" % checksum)
                fe.write("      CEDA CHECKSUM  : %s \n" % file_checksum)

        else:
            with open(log_file, 'a') as fe:
                fe.write("FAIL: %s, %s, %s \n" % (id, variable, datanode))
                fe.write("      LATEST CHECKSUM: %s \n" % checksum)
                fe.write("      CEDA CHECKSUM  : %s \n" % file_checksum)

def get_valid_models():

    institutes = [str(x[0]).strip() for x in Dataset.objects.values_list('institute').distinct()]
    valid_models_for_institute = {}
    for ins in institutes:
        valid_models_for_institute[ins] = [str(x[0]).strip() for x in  Dataset.objects.filter(institute=ins).values_list('model').distinct()]

    for ins, model in valid_models_for_institute.iteritems():
         print ins, model

def qc_set_max_test():

    ncfile = 'psl_Amon_FGOALS-g2_historical_r2i1p1_190001-190912.nc'
    var, table, model, expt, ens = ncfile.split('_')[:-1]

    # datafiles = []
    # for df in DataFile.objects.filter(dataset__variable=var, dataset__cmor_table=table, dataset__model=model,
    #                                   dataset__experiment=expt, dataset__ensemble=ens, timeseries=True):
    #     datafiles.append(df.ncfile)
    timeseries_df_errors = {}

    for datafile in DataFile.objects.filter(dataset__variable=var, dataset__cmor_table=table, dataset__model=model,
                                            dataset__experiment=expt, dataset__ensemble=ens, timeseries=True):
        timeseries_df_errors[datafile.ncfile] = get_total_qc_errors(datafile.ncfile)

    max_errors = max_timeseries_qc_errors(timeseries_df_errors)

    print max_errors

def max_timeseries_qc_errors(ts):
    """
    Input is of the format of a dictionary of dictonary e.g.
    {'filename': {'global': 0, 'variable': 1, 'other', 1}}
    :param ts:
    :return:
    """

    max_errors = {'global': 0, 'variable': 0, 'other': 0}

    for key in ['global', 'variable', 'other']:
        errors = []
        for file, errs in ts.iteritems():
            errors.append(errs[key])
        max_errors[key] = max(errors)

    return max_errors

def get_total_qc_errors(qcfile):
    files = DataFile.objects.filter(ncfile=qcfile)
    if files > 1:
       raise Exception("Length of files %s must be 1" % qcfile)

    file = files.first()
    qc_errors = file.qcerror_set.all()
    errors = {}
    errors['global'] = qc_errors.filter(error_type='global').count()
    errors['variable'] = qc_errors.filter(error_type='variable').exclude(error_msg__contains="ERROR (4)").count()
    errors['other'] = qc_errors.filter(error_type='other').exclude(error_msg__contains="ERROR (4)").count()

    return errors


def qc_list_test():

    for dataset in Dataset.objects.all():
        datafiles = dataset.datafile_set.all()
        for dfile in datafiles:
            qc_errors = dfile.qcerror_set.all()
            for error in qc_errors:
                path = error.file.archive_path
                file = error.file.ncfile


def add_ncfilename():

    data_specs = DataSpecification.objects.all()
    for dspec in data_specs:
        datasets = dspec.dataset_set.all()
        for dataset in datasets:
            datafiles = dataset.datafile_set.all()
            for dfile in datafiles:
                print dfile.archive_path
                dfile.ncfile = os.path.basename(dfile.archive_path)
                dfile.save()


def test_qc_cedacc():

    for dataset in Dataset.objects.filter(data_spec__datarequesters__requested_by__contains='CP4CDS'):
       datafiles = dataset.datafile_set.all()
       for d_file in datafiles:
           qcfile = str(d_file.archive_path)
           run_cf_checker(qcfile, d_file)
           run_cf_checker(qcfile, d_file)


           facets = qcfile.split('/')[6:12]
           odir = os.path.join('/usr/local/cp4cds-app/ceda-cc-log-files/', *facets)
           if qcfile:
               # Run CEDA-CC, including parsing of output and recording of error output
               run_ceda_cc(qcfile, d_file, odir)


def test_qc():

    project = "CP4CDS"
    data_specs = DataSpecification.objects.filter(datarequesters__requested_by__contains=project)
    LOGFILE = '../cp4cds_filelist2.log'

    for dspec in data_specs:
       datasets = dspec.dataset_set.all()
       for dataset in datasets:
           #dsid = dataset.esgf_ds_id
           datafiles = dataset.datafile_set.all()
           for dfile in datafiles:
               print dfile.archive_path





    # qcfile = str(d_file.archive_path)
    # if qcfile:
    # print qcfile
    # if not d_file.md5_checksum:
    # md5 = commands.getoutput('md5sum %s' % qcfile).split(' ')[0]
    # d_file.md5_checksum = md5
    # # Run CEDA-CC, including parsing of output and recording of error output
    # if not QCcheck.objects.filter(file_qc=d_file).filter(qc_check_type='CEDA-CC'):
    # run_ceda_cc(qcfile, d_file, odir)

    # Run CF checker and record error output
    # if not QCcheck.objects.filter(file_qc=d_file).filter(qc_check_type='CF'):
    # run_cf_checker(qcfile, d_file)
    #
    #
    #  df = DataFile.objects.first()
    # ceda_cc = df.qccheck_set.all()
    #
    # for q in qc:
    #    print q.qc_check_type
    #     df_qc = df_qccheck_set.all()
    #
    #    for err in q.qcerror_set.all():
    #        print err.qc_error
    #

    # dataset = Dataset.objects.first()
    # datafiles = dataset.datafile_set.all()
    # for d_file in datafiles:
    #    qcfile = str(d_file.archive_path)
    #    run_cf_checker(qcfile, d_file)
    # #
    #    run_cf_checker(qcfile, d_file)
    #
    #    if qcfile:
    #        # Run CEDA-CC, including parsing of output and recording of error output
    #        run_ceda_cc(qcfile, d_file, '/usr/local/cp4cds-app/ceda-cc-log-files/')
    #
    #     Run CF checker and record error output
    #


def check_models():
   variables = ['tas', 'tasmax', 'tasmin', 'ps', 'uas', 'vas', 'pr']

   models_three_expts = {}
   for var in variables:
       specs = DataSpecification.objects.filter(variable=var, frequency='mon', cmor_table='Amon')

       for spec in specs.all():
           models = get_no_models_per_expt(spec, ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85'])
           models_three_expts[var] = models


   unique_models = set()
   for key, models in models_three_expts.iteritems():
       if len(unique_models) == 0:
           unique_models = set(models)
       else:
           unique_models.intersection_update(models)
   print unique_models


def volume_test():

    specs = DataSpecification.objects.filter(variable='tas', frequency='mon', cmor_table='Amon')
    for spec in specs.all():
       get_no_models_per_expt(spec, ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85'])
       #get_no_models_per_expt(spec, ['rcp60', 'rcp26', 'amip', 'rcp85', 'historical', 'rcp45', 'piControl'])


def qc_initial_testing():
    """
    Test for adding qc
    :return:
    """
    start_time = datetime.date(1900, 1, 1)
    end_time = datetime.date(1999, 12, 31)

    d_requester, _ = DataRequester.objects.get_or_create(requested_by='qc-tests')
    d_spec, _ = DataSpecification.objects.get_or_create(variable='var', cmor_table='table', frequency='freq')
    d_spec.datarequesters.add(d_requester)
    d_spec.save()

    # Create qc-test dataset
    ds, _ = Dataset.objects.get_or_create(project='qcTest', product='Ruth', institute='inst', model='model',
                                         experiment='expt', frequency='freq', realm='realm', cmor_table='table',
                                         ensemble='ensemble', variable='var', version='v')
    ds.data_spec.add(d_spec)
    ds.save()


    # Create qc-test datafile
    file = '/badc/cmip5/data/cmip5/output1/MOHC/HadGEM2-ES/historical/mon/atmos/Amon/r1i1p1/latest/tas/tas_Amon_HadGEM2-ES_historical_r1i1p1_198412-200511.nc'

    df, _ = DataFile.objects.get_or_create(dataset=ds, filepath='file3', archive_path=file,
                                          size='1', checksum='checksum', download_url='download_url',
                                          tracking_id='tracking_id', variable='var',
                                          cf_standard_name='variable_cf_name',
                                          variable_long_name='variable_long_name',
                                          variable_units='units', start_time=start_time, end_time=end_time,
                                          cf_compliance_score=0, ceda_cc_score=0, file_qc_score=0
                                          )

    perform_qc(df)

if __name__ == '__main__':

    # add_ncfilename()
    # qc_list_test()
    # qc_set_max_test()
    # get_valid_models()
    # check_latest_datasets()
    endswith()





"""
abcvars = ['tas', 'ts', 'tasmin', 'tasmax', 'psl', 'ps', 'uas', 'vas', 'sfcWind', 'hurs', 'huss', 'pr', 'prsn', 'prc',
            'evspsbl', 'tauu', 'tauv', 'rlds', 'rlus', 'rsds', 'rsus', 'rsdt', 'rsut', 'rlut', 'prw', 'clt', 'clwvi',
            'clivi', 'ccb', 'cct', 'hfls', 'hfss']

model_lev_vars = ['ta', 'ua', 'va', 'hus', 'hur', 'wap', 'zg']

model_lev_vars2 = ['cl', 'clw', 'cli', 'mc']

land_surface_vars = ['mrsos', 'mrso', 'mrfso', 'mrros', 'mrro', 'evspsblsoi', 'tran', 'mrlsl', 'tsl', 'cVeg', 'cLitter'
                     'cSoil', 'treeFrac', 'grassFrac', 'cropFrac', 'pastureFrac']

ocean = ['tos', 'sos', 'zos', 'sic', 'sit', 'snd', 'sim', 'tsice']

freqs = ['3hr', '6hr', 'day', 'mon']
freqs3 = ['3hr', 'day', 'mon']
freqs2 = ['day', 'mon']

for var in ocean:
    for frequency in freqs2:
        specs = DataSpecification.objects.filter(variable=var, frequency=frequency)
        for spec in specs.all():
            models = get_no_models_per_expt(spec, ['historical', 'rcp45'])

    def volume(self):

        #testing data volumes
        specs = DataSpecification.objects.filter(variable='tas', frequency='mon', cmor_table='Amon')
        for spec in specs.all():
            get_no_models_per_expt(spec, ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85'])
            #get_no_models_per_expt(spec, ['rcp60', 'rcp26', 'amip', 'rcp85', 'historical', 'rcp45', 'piControl'])
            get_no_models_per_expt(spec, ['historical', 'rcp45', 'piControl'])



    #'quiet_cfchecker': testing.quiet_cfchecker(CFChecker, STANDARDNAME, AREATYPES, version, qcfile),
    #'filename_parser': testing.filename_parser(),
    #'ceda_cc_test': testing.ceda_cc_test(),

    def volume(self):

        #testing data volumes
        specs = DataSpecification.objects.filter(variable='tas', frequency='mon', cmor_table='Amon')
        for spec in specs.all():
            get_no_models_per_expt(spec, ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85'])
            #get_no_models_per_expt(spec, ['rcp60', 'rcp26', 'amip', 'rcp85', 'historical', 'rcp45', 'piControl'])
            get_no_models_per_expt(spec, ['historical', 'rcp45', 'piControl'])


    def filename_parser(self):
        dsid = 'CMIP5.output1.MIROC.MIROC5.rcp26.mon.atmos.Amon.r2i1p1.tas.20120710'
        dsid = dsid.replace('CMIP5', 'cmip5')
        dsid = dsid.split('.')
        del dsid[-2]
        version = 'v' + dsid[-1]
        dsid.insert(-1, version)
        del dsid[-1]
        dsid = os.path.join('', *dsid)

        dsid = '/badc/cmip5/data/' + dsid
        print dsid
        odir = '/usr/local/cp4cds-app/ceda-cc-output-test'

        qcfile = '/badc/cmip5/data/cmip5/output1/MIROC/MIROC5/rcp26/mon/atmos/Amon/r2i1p1/v20120710/cfc12global/cfc12global_Amon_MIROC5_rcp26_r2i1p1_200601-210012.nc'
        version = newest_version

#    def quiet_cfchecker(CFChecker, STANDARDNAME, AREATYPES, version, qcfile):
#    #   version = CFChecker.getFileCFVersion
#        cf = CFChecker(cfStandardNamesXML=STANDARDNAME, cfAreaTypesXML=AREATYPES, silent=True)
#        resp = cf.checker(qcfile)
#
#        for k,v in resp.items():
#            print k
#            print v

    def ceda_cc_test(self):
        for path, dir, file in os.walk(dsid):
            for f in file:
                file = os.path.join(path, f)
                print file
                argslist = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', odir, '--cae', '--blfmode', 'a']
                m = c4.main(argslist)
                print m
        #        print help(m)
"""

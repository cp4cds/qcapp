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

from qcapp.models import *
from app_functions import *
import argparse
import commands


#for dataset in Dataset.objects.filter(data_spec__datarequesters__requested_by__contains='CP4CDS'):
#    datafiles = dataset.datafile_set.all()
#    for d_file in datafiles:
#        qcfile = str(d_file.archive_path)
    #    run_cf_checker(qcfile, d_file)
    ##
    #    run_cf_checker(qcfile, d_file)


#        facets = qcfile.split('/')[6:12]
#        odir = os.path.join('/usr/local/cp4cds-app/ceda-cc-log-files/', *facets)
#        if qcfile:
            # Run CEDA-CC, including parsing of output and recording of error output
#            run_ceda_cc(qcfile, d_file, odir)

    # Run CF checker and record error output



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





#qcfile = str(d_file.archive_path)
#if qcfile:
#print qcfile
#if not d_file.md5_checksum:
#md5 = commands.getoutput('md5sum %s' % qcfile).split(' ')[0]
#d_file.md5_checksum = md5
## Run CEDA-CC, including parsing of output and recording of error output
#if not QCcheck.objects.filter(file_qc=d_file).filter(qc_check_type='CEDA-CC'):
#run_ceda_cc(qcfile, d_file, odir)

# Run CF checker and record error output
#if not QCcheck.objects.filter(file_qc=d_file).filter(qc_check_type='CF'):
#run_cf_checker(qcfile, d_file)


 #df = DataFile.objects.first()
#ceda_cc = df.qccheck_set.all()

#for q in qc:
#    print q.qc_check_type
    #df_qc = df_qccheck_set.all()

#    for err in q.qcerror_set.all():
#        print err.qc_error





#
# dataset = Dataset.objects.first()
#datafiles = dataset.datafile_set.all()
#for d_file in datafiles:
#    qcfile = str(d_file.archive_path)
#    run_cf_checker(qcfile, d_file)
##
#    run_cf_checker(qcfile, d_file)

#    if qcfile:
#        # Run CEDA-CC, including parsing of output and recording of error output
#        run_ceda_cc(qcfile, d_file, '/usr/local/cp4cds-app/ceda-cc-log-files/')

    # Run CF checker and record error output



#def check_models():
#    variables = ['tas', 'tasmax', 'tasmin', 'ps', 'uas', 'vas', 'pr']
#
#    models_three_expts = {}
#    for var in variables:
#        specs = DataSpecification.objects.filter(variable=var, frequency='mon', cmor_table='Amon')
#
#        for spec in specs.all():
#            models = get_no_models_per_expt(spec, ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85'])
#            models_three_expts[var] = models


#    unique_models = set()
#    for key, models in models_three_expts.iteritems():
#        if len(unique_models) == 0:
#            unique_models = set(models)
#        else:
#            unique_models.intersection_update(models)
#    print unique_models

#class AppTests:

#    def volume(self):

        #testing data volumes
#        specs = DataSpecification.objects.filter(variable='tas', frequency='mon', cmor_table='Amon')
#        for spec in specs.all():
#            get_no_models_per_expt(spec, ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85'])
#            #get_no_models_per_expt(spec, ['rcp60', 'rcp26', 'amip', 'rcp85', 'historical', 'rcp45', 'piControl'])


#    def qc(self):
#        """
#        Test for adding qc
#        :return:
#        """
#        start_time = datetime.date(1900, 1, 1)
#        end_time = datetime.date(1999, 12, 31)
#
#        d_requester, _ = DataRequester.objects.get_or_create(requested_by='qc-tests')
#        d_spec, _ = DataSpecification.objects.get_or_create(variable='var', cmor_table='table', frequency='freq')
#        d_spec.datarequesters.add(d_requester)
#        d_spec.save()

        # Create qc-test dataset
#        ds, _ = Dataset.objects.get_or_create(project='qcTest', product='Ruth', institute='inst', model='model',
#                                              experiment='expt', frequency='freq', realm='realm', cmor_table='table',
#                                              ensemble='ensemble', variable='var', version='v')
#        ds.data_spec.add(d_spec)
#        ds.save()


        # Create qc-test datafile
#        file = '/badc/cmip5/data/cmip5/output1/MOHC/HadGEM2-ES/historical/mon/atmos/Amon/r1i1p1/latest/tas/tas_Amon_HadGEM2-ES_historical_r1i1p1_198412-200511.nc'
#
#        df, _ = DataFile.objects.get_or_create(dataset=ds, filepath='file3', archive_path=file,
#                                               size='1', checksum='checksum', download_url='download_url',
#                                               tracking_id='tracking_id', variable='var',
#                                               cf_standard_name='variable_cf_name',
#                                               variable_long_name='variable_long_name',
#                                               variable_units='units', start_time=start_time, end_time=end_time,
#                                               cf_compliance_score=0, ceda_cc_score=0, file_qc_score=0
#                                               )

#        perform_qc(df)



#if __name__ == '__main__':#

#    check_models()

    # instantiate class AppTests
#    testing = AppTests()
#
#    function_map = {'volume': volume}
#        'qc': testing.qc()}
#        'volume': volume}
#    }
#

#    parser = argparse.ArgumentParser()
#    parser.add_argument('command', nargs=1)
#    args = parser.parse_args()
#    function = function_map[args.command[0]]


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
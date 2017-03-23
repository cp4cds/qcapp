#from django.test import TestCase

# Create your tests here.

import django

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


#testing data volumes
spec = DataSpecification.objects.first()
get_no_models_per_expt(spec, ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85'])
get_no_models_per_expt(spec, ['rcp60', 'rcp26', 'amip', 'rcp85', 'historical', 'rcp45', 'piControl'])


"""
dsid = 'CMIP5.output1.MIROC.MIROC5.rcp26.mon.atmos.Amon.r2i1p1.tas.20120710'
dsid = dsid.replace('CMIP5','cmip5')
dsid = dsid.split('.')
del dsid[-2]
version = 'v'+dsid[-1]
dsid.insert(-1, version)
del dsid[-1]
dsid = os.path.join('', *dsid)

dsid = '/badc/cmip5/data/' + dsid
print dsid
odir = '/usr/local/cp4cds-app/ceda-cc-output-test'


qcfile = '/badc/cmip5/data/cmip5/output1/MIROC/MIROC5/rcp26/mon/atmos/Amon/r2i1p1/v20120710/cfc12global/cfc12global_Amon_MIROC5_rcp26_r2i1p1_200601-210012.nc'
version = newest_version



def quiet_cfchecker(CFChecker, STANDARDNAME, AREATYPES, version, qcfile):
#   version = CFChecker.getFileCFVersion
    cf = CFChecker(cfStandardNamesXML=STANDARDNAME, cfAreaTypesXML=AREATYPES, silent=True)
    resp = cf.checker(qcfile)

    for k,v in resp.items():
        print k
        print v


quiet_cfchecker(CFChecker, STANDARDNAME, AREATYPES, version, qcfile)

for path, dir, file in os.walk(dsid):
    for f in file:
        file = os.path.join(path, f)
        print file
        argslist = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', odir, '--cae', '--blfmode', 'a']
        m = c4.main(argslist)
        print m
#        print help(m)
"""

#from django.test import TestCase

# Create your tests here.

import django

django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg
import collections, os, timeit, datetime
import requests, itertools
# IMPORT QC STUFF (only works in venv27)
from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import netCDF4
requests.packages.urllib3.disable_warnings()

dsid = 'CMIP5.output1.MIROC.MIROC5.rcp26.mon.atmos.Amon.r1i1p1.tas.20120710'
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


argslist = ['-p', 'CMIP5', '-D', dsid, '--ld', odir]
m = c4.main()

#for path, dir, file in os.walk(dsid):
#    for f in file:
#        file = os.path.join(path, f)
#        print file
#        m = c4.main(args=['-p', 'CMIP5',
#                          '-f', file,
#                          '--ld', odir,
#                          '--flfmode', 'wo'])
#        print m
#        print help(m)

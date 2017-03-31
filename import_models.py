import django
django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg
import collections, os, timeit, datetime, time
import requests, itertools
# IMPORT QC STUFF (only works in venv27)
from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings()
from app_functions import *
print "Counts..."
print "Dataset:", Dataset.objects.count()
print "DataFile:", DataFile.objects.count()

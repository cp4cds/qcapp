

import django
django.setup()
import os
import re
import fnmatch
import json
import requests
import commands
import datetime
import argparse
import filecmp
import uuid
import shutil

from django.db.models import Q
from netCDF4 import Dataset as ncDataset ## Conflict with local naming convention Dataset is a db model!!
from subprocess import call
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from sys import argv
from qcapp.models import *
from utils import *
from qc_settings import *
from qc_functions import *
requests.packages.urllib3.disable_warnings()

ARCHIVE_ROOT = "/badc/cmip5/data/"
WEBROOT = "http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/"
ARCHIVE_BASEDIR = "/badc/cmip5/data/cmip5/output1/"
GWS_BASEDIR = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/"
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"
NEW_DATA_DIR = "/group_workspaces/jasmin2/cp4cds1/data/corrected/v20180618"







if __name__ == "__main__":
    """
    This script will 
    1. Redo a CF and CEDA-CC check on the file
    2. Check the output contains no errors
    3. Will then copy this file back into the gws-archive with a new version
    4. Will add this to CP4CDS database
    5. Will append to a list of new files to be published to the CP4CDS index node
    """
    main()


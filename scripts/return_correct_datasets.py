
from setup_django import *
import sys
import re
from settings import *
from utils import *
# from ceda_cc import c4
# from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version
# from time_checks.run_file_timechecks import main as single_file_time_checks
# from time_checks.run_multifile_timechecks import main as multi_file_time_checks
# from is_latest import check_datafile_is_latest
# import subprocess
# from utils import *
# from qc_functions import *
# from is_latest import *
#


failed_datasets = Dataset.objects.exclude(qc_passed=True, version='v20181201')

for dataset in failed_datasets:

    qc_ok = True

    for df in dataset.datafile_set.all():
        if not df.qc_passed:
            qc_ok = False
            continue

    if qc_ok:
        print dataset

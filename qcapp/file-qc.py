import django

django.setup()

import datetime
import os
from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

# IMPORT QC STUFF (only works in venv27)
from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version

def file_qc():
    qcfile = '/group_workspaces/jasmin/cp4cds1/data/initial/cmip5/output1/MOHC/HadGEM2-ES/rcp45/mon/atmos/Amon/r1i1p1/v20110329/zg/zg_Amon_HadGEM2-ES_rcp45_r1i1p1_200512-203011.nc'

    run_cf_checker(qcfile)
    run_ceda_cc(qcfile)
    parse_ceda_cc()

def run_cf_checker(qcfile):
    cf = CFChecker(cfStandardNamesXML=STANDARDNAME, cfAreaTypesXML=AREATYPES, version=newest_version)
    resp = cf.checker(qcfile)


def run_ceda_cc(qcfile):
    # run the ceda-cc - generate the qcBatch log.
    # write list to a file for use by ceda-cc
    odir = '/usr/local/cp4cds-app/ceda-cc-output'

    m = c4.main( args=['-p', 'CMIP5', '-f', qcfile, '--ld', odir])
    # need to now parse output in the odir

def parse_ceda_cc():
    """Parse the qcBatch log for fails

    Makes a dictotionary with arrivals file path and status
    Run the bactch log parser to work out where the file is going.
      - good files are marked as good
      - Minor fail - just one fail - Work out manually / contact data provider /manually pass
      - Fail - more than filure reported by ceda-cc - Work out manually / contact data provider
      - Exceptions - where ceda-cc trows an exception - Work out manually
    """
#(venv27)[badc@ingest1 ccmi]$ more /datacentre/processing/ccmi-1/sams_ingest/single_file/logs_02/qcBatchLog_20160831_131325.txt
#2016-08-31 14:13:25,949 INFO Starting batch -- number of file: 1
#2016-08-31 14:13:25,950 INFO Source: /datacentre/arrivals/users/schumar/ccmi1_MESSy_v1/CCMI1/output/MESSy/EMAC-L47MA/refC2/daily/atmos/lt10co/r1i1p1/lt10co_satdaily_EMAC-L47MA_refC2_r1i1p1_20440101-20441231.nc
#2016-08-31 14:13:25,951 INFO Command: /usr/local/ingest_software/venv27/bin/ceda-cc -p CCMI --cae -f /datacentre/arrivals/users/schumar/ccmi1_MESSy_v1/CCMI1/output/MESSy/EMAC-L47MA/refC2/daily/atmos/lt10co/r1i
#1p1/lt10co_satdaily_EMAC-L47MA_refC2_r1i1p1_20440101-20441231.nc
#2016-08-31 14:13:26,220 INFO Python netcdf: cdms2
#2016-08-31 14:13:26,220 INFO Starting: lt10co_satdaily_EMAC-L47MA_refC2_r1i1p1_20440101-20441231.nc
#2016-08-31 14:13:26,221 INFO Starting file lt10co_satdaily_EMAC-L47MA_refC2_r1i1p1_20440101-20441231.nc
#2016-08-31 14:13:26,221 INFO C4.001.001: [parse_filename]: OK
#2016-08-31 14:13:26,221 INFO C4.001.002: [parse_filename_timerange]: OK
#2016-08-31 14:13:26,221 INFO C4.001.004: [file_name_extra]: OK
#2016-08-31 14:13:27,091 INFO C4.002.001: [global_ncattribute_present]: OK
#2016-08-31 14:13:27,091 INFO C4.002.003: [variable_type]: OK
#2016-08-31 14:13:27,091 INFO C4.002.004: [variable_ncattribute_present]: OK
#2016-08-31 14:13:27,109 INFO C4.002.005: [variable_ncattribute_mipvalues]: OK
#2016-08-31 14:13:27,109 INFO C4.002.005: [variable_ncattribute_mipvalues]: OK
#2016-08-31 14:13:27,119 INFO C4.002.005: [variable_ncattribute_mipvalues]: OK
#2016-08-31 14:13:27,119 ERROR C4.002.006: [global_ncattribute_cv]: FAILED:: Global attributes do not match constraints: [('frequency', 'daily', "['fx', 'yr', 'mon', 'day']")]
#2016-08-31 14:13:27,121 INFO C4.002.007: [filename_filemetadata_consistency]: OK
#2016-08-31 14:13:27,121 INFO Done -- error count 1

    logfiles = glob.glob("logs_02/**")
    if len(logfiles) != 1:
        log("unexpected number of ceda-cc output files")
        return "ERROR"

    logfile = logfiles[0]

    for line in open(logfile):
        match = re.search(r'Done -- error count (\d+)', line)
        if match:
            errors = int(match.group(1))
        match = re.search(r'ERROR Exception has occured', line)
        if match:
            return "exception"
        match = re.search(r'INFO Done -- testing aborted because of severity of errors', line)
        if match:
            return "abort"
        match = re.search(r'ERROR C4\.\d{3}\.\d{3}: (.*)', line)
        if match:
            lasterror = match.groups()[0]


    if errors == 0:
        return "pass"
    if errors == 1:
        return lasterror
    if errors > 1 :
        return "fail"



if __name__ == '__main__':
    file_qc()
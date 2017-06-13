import django

django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

import collections, os, timeit, datetime, time, re, glob
import commands
import requests, itertools

from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version

ARCHIVE_ROOT = "/badc/cmip5/data/"
GWSDIR = "/group_workspaces/jasmin/cp4cds1/qc/CFchecks/CF-OUTPUT/"

def read_cf_checker_output(file, d_file):
    """
    :param file: File to quality control 
    (format /badc/cmip5/data/cmip5/output1/
    <institute>/<model>/<experiment>/<frequency>/<realm>/<table>/<ensemble>/<version>/<variable>/<filename>)
    :param d_file: associated DataFile record
    :return: 
    """


def run_cf_checker(qcfile, d_file):
    """

    :param qcfile: single file with full CEDA archive path to be checked
    :param d_file: the DataFile object to link the CF results to

    :result The results of the CF-Checker are stored in the QCerror and QCchecks tables 

    Run the CF checker in in-line mode with auto-version of file detection for a single file.
    The following options are used in the CF-Checker:
        cfStandardNamesXML=STANDARDNAME, 
        cfAreaTypesXML=AREATYPES, 
        version=CFVersion(), 
        silent=True 


    """

    #    print ""
    #    print ""
    #    print "RUNNING CF CHECKER", qcfile
    #    print ""
    #    print ""
    STANDARDNAME = '/usr/local/cp4cds-app/cf-checker/cf-standard-name-table.xml'
    AREATYPES = '/usr/local/cp4cds-app/cf-checker/area-type-table.xml'
    cf = CFChecker(cfStandardNamesXML=STANDARDNAME, cfAreaTypesXML=AREATYPES, version=CFVersion(), silent=True)

    #    start_time = timeit.default_timer()

    try:
        resp = cf.checker(qcfile)

        #    time_taken = timeit.default_timer() - start_time
        #    print "Time taken for a single file CF check is ", time_taken

        error_msgs = []
        vars = resp.items()[0]
        for message in vars[1].values():
            if len(message['FATAL']) > 0:
                error_msgs.append('FATAL: ' + message['FATAL'][0])
            if len(message['ERROR']) > 0:
                error_msgs.append('ERROR: ' + message['ERROR'][0])
            if len(message['WARN']) > 0:
                error_msgs.append('WARNING: ' + message['WARN'][0])
            if len(message['INFO']) > 0:
                error_msgs.append('INFO: ' + message['INFO'][0])

        gll = resp.items()[1]
        if gll[1]['FATAL']:
            error_msgs.append('FATAL: ' + gll[1]['FATAL'][0])
        if gll[1]['ERROR']:
            error_msgs.append('ERROR: ' + gll[1]['ERROR'][0])
        if gll[1]['WARN']:
            error_msgs.append('WARN: ' + gll[1]['WARN'][0])
        if gll[1]['INFO']:
            error_msgs.append('INFO: ' + gll[1]['INFO'][0])

    except:
        error_msgs = ['FATAL']

    if len(error_msgs) > 0:
        qc_check_table, _ = QCcheck.objects.get_or_create(file_qc=d_file, qc_check_type='CF')

        for err in error_msgs:
            qc_err, _ = QCerror.objects.get_or_create(qc_error=err)
            qc_check_table.qc_error.add(qc_err)
        qc_check_table.save()


def run_ceda_cc(file, d_file, odir):
    """
    Run CEDA-CC on a single file with the following options, generating a qcBatch log
        -p CMIP5
        -f file
        --log multi
        --ld 
        --cae
        --blfmode a

    Output is written to log file directly parsed and (TODO deleted)

    :result: CEDA-CC errors recorded in QCerror
    """

    print file

    # Run CEDA-CC
    ceda_cc_file = glob.glob('{0}/*/{1}__qclog_*.txt'.format(odir, file.split('/')[-1][:-3]))
    error_msgs = []

    if len(ceda_cc_file) == 0:
        error_msgs.append('File not found %s' % file)
    else:
        if not ceda_cc_file[0]:
            cedacc_args = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', odir, '--cae', '--blfmode', 'a']
            _ = c4.main(cedacc_args)

        # CEDA-CC filename
        # ceda_cc_file = glob.glob('{0}/{1}__qclog_*.txt'.format(odir, file.split('/')[-1][:-3]))
        # odir + '/' + file.split('/')[-1][:-3] + '__qclog_' + time.strftime("%Y%m%d") + '.txt'


        # Read in CEDA-CC output
        with open(ceda_cc_file[0], 'r') as reader:
            ceda_cc_out = reader.readlines()

        # Identify where CEDA-CC picks up a QC error
        cedacc_error = re.compile('.*FAILED::.*')
        cedacc_exception = re.compile('.*Exception.*')
        cedacc_abort = re.compile('.*aborted.*')

        for line in ceda_cc_out:
            if cedacc_error.match(line.strip()):
                error_msgs.append(line)
            if cedacc_exception.match(line.strip()):
                error_msgs.append(line)
            if cedacc_abort.match(line.strip()):
                error_msgs.append(line)

                # Make a CEDA-CC qc_check table and qc_error tables for all CEDA-CC errors

    if len(error_msgs) > 0:
        qc_check_table, _ = QCcheck.objects.get_or_create(file_qc=d_file, qc_check_type='CEDA-CC')

        for err in error_msgs:
            qc_err, _ = QCerror.objects.get_or_create(qc_error=err)
            qc_check_table.qc_error.add(qc_err)
        qc_check_table.save()


def perform_qc(project):
    """
    Perform the quality control
    Generate CEDA-CC files and parse output
    Perform CF-checks

    :return:
    """
    data_specs = DataSpecification.objects.filter(datarequesters__requested_by__contains=project)

    for dspec in data_specs:
        datasets = dspec.dataset_set.all()
        for dataset in datasets:
            dsid = dataset.esgf_ds_id
            odir = os.path.join('/usr/local/cp4cds-app/ceda-cc-log-files', *dsid.split('.')[2:])
            if not os.path.isdir(odir):
                os.makedirs(odir)

            datafiles = dataset.datafile_set.all()
            for d_file in datafiles:
                qcfile = str(d_file.archive_path)
                if qcfile:
                    print qcfile

                    if not d_file.md5_checksum:
                        md5 = commands.getoutput('md5sum %s' % qcfile).split(' ')[0]
                        d_file.md5_checksum = md5
                    # Run CEDA-CC, including parsing of output and recording of error output
                    if not QCcheck.objects.filter(file_qc=d_file).filter(qc_check_type='CEDA-CC'):
                        run_ceda_cc(qcfile, d_file, odir)

                    # Run CF checker and record error output
                    if not QCcheck.objects.filter(file_qc=d_file).filter(qc_check_type='CF'):
                        run_cf_checker(qcfile, d_file)

                    # RECORD SCORES to d_file
                    # d_file.cf_compliance_score
                    # d_file.ceda_cc_score
                    # d_file.file_qc_score
                    d_file.save()

if __name__ == '__main__':
    # These constraints will in time be loaded in via csv for multiple projects.
    project = 'CMIP5'
    node = "172.16.150.171"
    expts = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
    distrib = False
    latest = True
    file = '/usr/local/cp4cds-app/project-specs/cp4cds-dmp_data_request.csv'
    with open('cp4cds-file-error.log', 'w') as fe:
        fe.write('')
    # file = '/usr/local/cp4cds-app/project-specs/magic_data_request.csv'
    #    file = '/usr/local/cp4cds-app/project-specs/abc4cde_data_request.csv'
    generate_data_records(project, node, expts, file, distrib, latest)
# url = "https://172.16.150.171/esg-search/search?type=File&project=CMIP5&variable=tas&cmor_table=Amon&time_frequency=mon&model=HadGEM2-ES&experiment=historical&ensemble=r1i1p1&latest=True&distrib=False&format=application%%2Fsolr%%2Bjson&limit=10000"
#    project = 'CP4CDS'
#    perform_qc(project)


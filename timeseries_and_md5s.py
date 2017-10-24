import django

django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

import collections, os, timeit, datetime, time, re, glob
import commands
import requests, itertools

from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version



def main(project):

    data_specs = DataSpecification.objects.filter(datarequesters__requested_by__contains=project)

    for dspec in data_specs:
        datasets = dspec.dataset_set.all()
        for dataset in datasets:

            datafiles = dataset.datafile_set.all()
            for d_file in datafiles:

                file = d_file.archive_path
                print file

                if os.path.isdir(os.path.dirname(file)):

                    if len(os.listdir(os.path.dirname(file))) > 1:
                        d_file.timeseries = True
                    else:
                        d_file.timeseries = False

                    if not d_file.md5_checksum:
                        md5 = commands.getoutput('md5sum %s' % file).split(' ')[0]
                        d_file.md5_checksum = md5

                    d_file.save()

                else:
                    log_file = 'missing_dirs_20170616.log'
                    if not os.path.isfile(log_file):
                        with open(log_file, 'w') as logger:
                            logger.write(os.path.dirname(file))
                    else:
                        with open(log_file, 'a') as logger:
                            logger.write(os.path.dirname(file))

if __name__ == '__main__':
    # These constraints will in time be loaded in via csv for multiple projects.
    project = 'CMIP5'
    node = "172.16.150.171"
    expts = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
    distrib = False
    latest = True
    file = '/usr/local/cp4cds-app/project-specs/cp4cds-dmp_data_request.csv'
    # file = '/usr/local/cp4cds-app/project-specs/magic_data_request.csv'
    #    file = '/usr/local/cp4cds-app/project-specs/abc4cde_data_request.csv'
    # url = "https://172.16.150.171/esg-search/search?type=File&project=CMIP5&variable=tas&cmor_table=Amon&time_frequency=mon&model=HadGEM2-ES&experiment=historical&ensemble=r1i1p1&latest=True&distrib=False&format=application%%2Fsolr%%2Bjson&limit=10000"
    project = 'CP4CDS'
    main(project)
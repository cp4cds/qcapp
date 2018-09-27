# URL TEMPLATES
# These constraints will in time be loaded in via csv for multiple projects.
# url = "https://172.16.150.171/esg-search/search?type=File&project=CMIP5&variable=tas&cmor_table=Amon&
# time_frequency=mon&model=HadGEM2-ES&experiment=historical&ensemble=r1i1p1&latest=True&distrib=False&
# format=application%%2Fsolr%%2Bjson&limit=10000"

"""

This file contains constants, global variables and templates

"""

QCAPP_PATH = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/"
ARCHIVE_ROOT = "/badc/cmip5/data/"
GWSDIR = "/group_workspaces/jasmin2/cp4cds1/qc/CFchecks/CF-OUTPUT/"
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"
QCLOGS = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/QC_LOGS"
DATAFILE_LATEST_CACHE = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_LATEST_CACHE"
AREATABLE = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/area-type-table.xml"
STDNAMETABLE = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/cf-standard-name-table.xml"
FILELIST = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/ancil_files/cp4cds_all_vars.txt"
ALLEXPTS = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']


NO_FILE_LOG = 'log_dir/cp4cds_nofile_error.log'
CEDACC_DIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/CEDACC_LOGS"
CF_DIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/CF-LOGS/"
CF_FATAL_DIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/CF-FATAL-LOGS"
TC_DIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/TIMECHECKS-LOGS/"
TS_DIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/TIMESERIES-OUTPUT/"
DATASET_LATEST_CACHE = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/LATEST_DATASET_CACHE"
DATASET_LATEST_DIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/LATEST_DATASET_LOGS"
DATAFILE_LATEST_DIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/LATEST_DATAFILE_LOGS"
#    node = "172.16.150.171"
node = "esgf-index1.ceda.ac.uk"
distrib = False
latest = True
requester = "CP4CDS"
DEBUG = False
project = "CMIP5"

URL_DS_MODEL_FACETS = 'https://%(node)s/esg-search/' \
                      'search?type=Dataset&' \
                      'project=%(project)s&' \
                      'variable=%(variable)s&' \
                      'cmor_table=%(table)s&' \
                      'time_frequency=%(frequency)s&' \
                      'experiment=%(experiment)s&' \
                      'latest=%(latest)s&distrib=%(distrib)s&' \
                      'facets=model&' \
                      'format=application%%2Fsolr%%2Bjson'

URL_DS_ENSEMBLE_FACETS = 'https://%(node)s/esg-search/' \
                         'search?type=Dataset&' \
                         'project=%(project)s&' \
                         'variable=%(variable)s&' \
                         'cmor_table=%(table)s&' \
                         'time_frequency=%(frequency)s&' \
                         'model=%(model)s&' \
                         'experiment=%(experiment)s&' \
                         'latest=%(latest)s&distrib=%(distrib)s&' \
                         'facets=ensemble&' \
                         'format=application%%2Fsolr%%2Bjson'

URL_DATASET_ENSEMBLE = 'https://%(node)s/esg-search/' \
                       'search?type=Dataset&' \
                       'project=%(project)s&' \
                       'variable=%(variable)s&' \
                       'cmor_table=%(table)s&' \
                       'time_frequency=%(frequency)s&' \
                       'model=%(model)s&' \
                       'ensemble=%(ensemble)s&' \
                       'experiment=%(experiment)s&' \
                       'latest=%(latest)s&distrib=%(distrib)s&' \
                       'format=application%%2Fsolr%%2Bjson&limit=10000'

URL_FILE_INFO = 'https://%(node)s/esg-search/' \
                'search?type=File&' \
                'project=%(project)s&' \
                'variable=%(variable)s&' \
                'cmor_table=%(table)s&' \
                'time_frequency=%(frequency)s&' \
                'model=%(model)s&' \
                'experiment=%(experiment)s&' \
                'ensemble=%(ensemble)s&' \
                'latest=%(latest)s&distrib=%(distrib)s&' \
                'format=application%%2Fsolr%%2Bjson&limit=10000'



URL_FILE_INFO_AT_CEDA = 'https://%(node)s/esg-search/search?type=File&project=%(project)s&title=%(title)s&data_node=%(datanode)s&format=application%%2Fsolr%%2Bjson&limit=10000'




URL_LATEST_DS_TEMPLATE = 'https://%(node)s/esg-search/' \
                         'search?type=Dataset&' \
                         'project=%(project)s&' \
                         'institute=%(institute)s&' \
                         'model=%(model)s&' \
                         'experiment=%(experiment)s&' \
                         'time_frequency=%(frequency)s&' \
                         'realm=%(realm)s&' \
                         'cmor_table=%(table)s&' \
                         'ensemble=%(ensemble)s&' \
                         '&distrib=%(distrib)s&' \
                         'format=application%%2Fsolr%%2Bjson&limit=10000'
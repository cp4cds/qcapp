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
GWS_BASEDIR = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/"
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
LOCAL_JSON_DF_CACHE = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/local_datafile_cache"
#    node = "172.16.150.171"
cedaindex = "esgf-index1.ceda.ac.uk"
requester = "CP4CDS"
DEBUG = False
project = "CMIP5"


model_dict = {}
model_dict['BCC'] = [u'bcc-csm1-1', u'bcc-csm1-1-m']
model_dict['BNU'] = [u'BNU-ESM']
model_dict['CCCma'] = [u'CanCM4', u'CanESM2', u'CanAM4']
model_dict['CMCC'] = [u'CMCC-CESM', u'CMCC-CM', u'CMCC-CMS']
model_dict['CNRM-CERFACS'] = [u'CNRM-CM5-2', u'CNRM-CM5']
model_dict['CSIRO-BOM'] = [u'ACCESS1-3', u'ACCESS1-0']
model_dict['CSIRO-QCCCE'] = [u'CSIRO-Mk3-6-0']
model_dict['FIO'] = [u'FIO-ESM']
model_dict['ICHEC'] = [u'EC-EARTH']
model_dict['INM'] = [u'inmcm4']
model_dict['IPSL'] = [u'IPSL-CM5B-LR', u'IPSL-CM5A-MR', u'IPSL-CM5A-LR']
model_dict['LASG-CESS'] = [u'FGOALS-g2']
model_dict['LASG-IAP'] = [u'FGOALS-s2']
model_dict['MOHC'] = [u'HadGEM2-ES', u'HadCM3', u'HadGEM2-CC', u'HadGEM2-A']
model_dict['MPI-M'] = [u'MPI-ESM-LR', u'MPI-ESM-MR', u'MPI-ESM-P']
model_dict['NASA-GISS'] = [u'GISS-E2-H', u'GISS-E2-R', u'GISS-E2-R-CC', u'GISS-E2-H-CC']
model_dict['NCAR'] = [u'CCSM4']
model_dict['NCC'] = [u'NorESM1-M', u'NorESM1-ME']
model_dict['NIMR-KMA'] = [u'HadGEM2-AO']
model_dict['NOAA-GFDL'] = [u'GFDL-HIRAM-C360', u'GFDL-ESM2M', u'GFDL-ESM2G', u'GFDL-CM2p1', u'GFDL-CM3', u'GFDL-HIRAM-C180']
model_dict['NSF-DOE-NCAR'] = [u'CESM1-BGC', u'CESM1-CAM5', u'CESM1-FASTCHEM', u'CESM1-CAM5-1-FV2', u'CESM1-WACCM']

table_freq_mapping = {}
table_freq_mapping['mon'] = ['LImon']
table_freq_mapping['mon'] = ['Lmon']
table_freq_mapping['3hr'] = ['3hr']
table_freq_mapping['mon'] = ['Amon']
table_freq_mapping['mon'] = ['OImon']
table_freq_mapping['day'] = ['day']
table_freq_mapping['mon'] = ['aero']
table_freq_mapping['mon'] = ['Omon']
table_freq_mapping['3hr'] = ['cf3hr']

table_realm_mapping = {}
table_realm_mapping['landIce'] = ['LImon']
table_realm_mapping['land'] = ['Lmon']
table_realm_mapping['atmos'] = ['3hr']
table_realm_mapping['atmos'] = ['Amon']
table_realm_mapping['seaIce'] = ['OImon']
table_realm_mapping['atmos'] = ['day']
table_realm_mapping['aerosol'] = ['aero']
table_realm_mapping['ocean'] = ['Omon']
table_realm_mapping['atmos'] = ['cf3hr']


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

# URL TEMPLATES
# These constraints will in time be loaded in via csv for multiple projects.
# url = "https://172.16.150.171/esg-search/search?type=File&project=CMIP5&variable=tas&cmor_table=Amon&
# time_frequency=mon&model=HadGEM2-ES&experiment=historical&ensemble=r1i1p1&latest=True&distrib=False&
# format=application%%2Fsolr%%2Bjson&limit=10000"


ARCHIVE_ROOT = "/badc/cmip5/data/"
GWSDIR = "/group_workspaces/jasmin/cp4cds1/qc/CFchecks/CF-OUTPUT/"
NO_FILE_LOG = 'log_dir/cp4cds_nofile_error.log'
CEDACC_DIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/CEDACC-OUTPUT/"
CF_DIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/CF-OUTPUT/"
AREATABLE = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/area-type-table.xml"
STDNAMETABLE = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/cf-standard-name-table.xml"
FILELIST = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/ancil_filescp4cds-files.txt"
DEBUG = True
#expts = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
#    node = "172.16.150.171"
node = "esgf-index1.ceda.ac.uk"
distrib = False
latest = True
requester = "CP4CDS"

URL_DS_MODEL_FACETS = 'https://%(node)s/esg-search/' \
                      'search?type=Dataset&' \
                      'project=%(project)s&' \
                      'variable=%(variable)s' \
                      '&cmor_table=%(table)s&' \
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

URL_LATEST_TEMPLATE = 'https://%(latest_node)s/esg-search/' \
                      'search?type=File&' \
                      'project=%(latest_project)s&' \
                      'model=%(model)s&' \
                      'experiment=%(experiment)s&' \
                      'time_frequency=%(frequency)s&' \
                      'cmor_table=%(table)s&' \
                      'ensemble=%(ensemble)s&' \
                      'variable=%(variable)s&' \
                      'latest=%(latest_latest)s&distrib=%(distrib_latest)s&replica=%(replica_latest)s&' \
                      'format=application%%2Fsolr%%2Bjson&limit=10000'


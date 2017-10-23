# URL TEMPLATES
# These constraints will in time be loaded in via csv for multiple projects.
# url = "https://172.16.150.171/esg-search/search?type=File&project=CMIP5&variable=tas&cmor_table=Amon&
# time_frequency=mon&model=HadGEM2-ES&experiment=historical&ensemble=r1i1p1&latest=True&distrib=False&
# format=application%%2Fsolr%%2Bjson&limit=10000"

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

URL_LATEST_TEMPLATE = 'https://%(node)s/esg-search/' \
                      'search?type=File&' \
                      'project=%(project)s&' \
                      'model=%(model)s&' \
                      'experiment=%(experiment)s&' \
                      'time_frequency=%(frequency)s&' \
                      'cmor_table=%(table)s&' \
                      'ensemble=%(ensemble)s&' \
                      'variable=%(variable)s&' \
                      'latest=%(latest)s&distrib=%(distrib_latest)s&replica=%(replica_latest)s&' \
                      'format=application%%2Fsolr%%2Bjson&limit=10000'

ARCHIVE_ROOT = "/badc/cmip5/data/"
GWSDIR = "/group_workspaces/jasmin/cp4cds1/qc/CFchecks/CF-OUTPUT/"
NO_FILE_LOG = 'cp4cds_nofile_error.log'


import django

django.setup()

import datetime
import os
from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg
import collections, os, timeit
from pyesgf.search import SearchConnection
import cProfile
import time
import requests
import itertools

import pdb;
def get_spec_info(project, variable, table, frequency, distrib, latest):
    """
    Uses ESGF pyclient to query ESGF and writes information for a given variable, table and frequency
    to the dataset and datafile models.

    :param variable: from data specification
    :param table:    from data specification
    :param frequency:from data specification
    :return: output to cache file
    """
    expts = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']

    # cp4cds-app1-test can't see ceda node?? for testing for now using dkrz
    conn = SearchConnection('http://esgf-data.dkrz.de/esg-search', distrib=distrib)
    ctx = conn.new_context(project='CMIP5', variable=variable, cmor_table=table, time_frequency=frequency, latest=True)

#    institutes = ctx.facet_counts['institute']
    institutes = ['MOHC']
#    for institute in institutes.keys():
    for institute in institutes:

        ctx_i = ctx.constrain(institute=institute)
        models = ctx_i.facet_counts['model']

        for model in models.keys():
            ctx_m = ctx_i.constrain(model=model)

            for expt in expts:
                ctx_e = ctx_m.constrain(experiment=expt)
                ensembles = ctx_e.facet_counts['ensemble']

                for ensemble in ensembles.keys():

                    realm = ctx_e.facet_counts['realm'].keys()[0].strip()
                    product = ctx_e.facet_counts['product'].keys()[0].strip()
                    experiment_family = ctx_e.facet_counts['experiment_family'].keys()[0].strip()
                    forcing = ctx_e.facet_counts['forcing'].keys()[0].strip()
                    version = ctx_e.facet_counts['version'].keys()[0].strip() # TODO not returning latest
                    variable_long_name = ctx_e.facet_counts['variable_long_name'].keys()[0].strip()
                    cf_standard_name = ctx_e.facet_counts['cf_standard_name'].keys()[0].strip()
                    print institute, model, expt, ensemble, realm, product, experiment_family, forcing, version, \
                          variable_long_name, cf_standard_name

                    """
                    MAKE A DATASET RECORD WITH THIS INFORMATION
                    """
                    create_dataset_record(project, product, institute, model, experiment, frequency, realm, table,
                                          ensemble, version, experiment_family, forcings)



                    """
                    NOW COLLECT THE DATAFILE INFORMATION


                    !!!!REQUIRED!!!
                    start_time = models.DateField()
                    end_time = models.DateField()
                    variable_units = file.variable_units
                    """
                    for ds in ctx_e.search():
                        files = ds.file_context().search()
                        for file in files:
                            filename = file.filename
                            download_url = file.download_url
                            size = file.size
                            checksum = file.checksum
                            tracking_id = file.tracking_id
                            data_node = file.index_node
                            """
                            NOW MAKE THE DATAFILE RECORD
                            """
                            create_datafile_record(filename, size, checksum, download_url, data_node, tracking_id,
                                                   variable, variable_cf_name, variable_long_name)


def create_dataset_record(project, product, institute, model, experiment, frequency, realm, table,
                          ensemble, version, experiment_family, forcings):

    ds, result = Dataset.objects.get_or_create(project=project,
                                               product=product,
                                               institute=institute,
                                               model=model,
                                               experiment=experiment,
                                               frequency=frequency,
                                               realm=realm,
                                               cmor_table=table,
                                               ensemble=ensemble,
                                               variable=variable,
                                               version=version,
                                               experiment_family=experiment_family,
                                               forcing=forcings
                                               )
    ds.save()

def create_datafile_record(filename, size, checksum, download_url, index_node, tracking_id,
                           variable, variable_cf_name, variable_long_name):
                           #variable_units, start_time, end_time):

    newfile, result = DataFile.objects.get_or_create(dataset=ds,
                                                     filename=filename,
                                                     size=size,
                                                     checksum=checksum,
                                                     download_url=download_url,
                                                     tracking_id=tracking_id,
                                                     data_node=index_node,
                                                     variable=variable,
                                                     cf_standard_name=variable_cf_name,
                                                     variable_long_name=variable_long_name,
                                                     #variable_units=variable_units,
                                                     #start_time=start_time,
                                                     #end_time=end_time
                                                     )

    newfile.save()




#    url = 'https://esgf-data.dkrz.de/esg-search/search?type=File&project=CMIP5&variable=tas&cmor_table=Amon&latest=True&distrib=False&facets=institute&format=application%%2Fsolr%%2Bjson'


if __name__ == '__main__':
    variable = 'tas'
    table = 'Amon'
    frequency = 'mon'
    project = 'cmip5'
    get_spec_info(project, variable, table, frequency, distrib=True, latest=True)
import django

django.setup()

import datetime
import os
from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

#import pdb;
def generate_requests_table(file):
    no_ens_membs = 1

    lineno = 0
    reader = open(file, 'r')
    data = reader.readlines()
    reader.close()
    for line in data:

        if lineno == 0:
           request_name = line.split(',')[0].strip()
        if lineno > 1:
            variable = line.split(',')[0].strip()
            cmor_table = line.split(',')[1].strip()
            frequency = line.split(',')[2].strip()
            variable_long_name = line.split(',')[3].strip()
            number_experiments = line.split(',')[4].strip()
            number_of_models = line.split(',')[5].strip()
            volume_of_data = line.split(',')[6].strip()

            create_request_record(request_name, variable, cmor_table, frequency, variable_long_name, number_experiments,
                                  number_of_models, volume_of_data, no_ens_membs)

        lineno += 1




def create_request_record(request_name, variable, cmor_table, frequency, variable_long_name, number_experiments,
                          number_of_models, volume_of_data, no_ens_membs):

    """
    Create a Request record
    """
  #  pdb.set_trace()
    req, result = DataSpecification.objects.get_or_create(requester=request_name,
                                                          variable=variable,
                                                          cmor_table=cmor_table,
                                                          frequency=frequency,
                                                          variable_long_name=variable_long_name,
                                                          number_experiments=number_experiments,
                                                          number_of_models=number_of_models,
                                                          volume_of_data=volume_of_data,
                                                          number_ensemble_members=no_ens_membs
                                                         )
    req.save()



if __name__ == '__main__':
    file = '/group_workspaces/jasmin/cp4cds1/data_availability/data_availability_summary_cp4cs.csv'
    generate_requests_table(file)
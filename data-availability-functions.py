import django
django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

import collections, os, timeit, datetime, time, re, glob
import commands
import requests, itertools

from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version




def get_no_models_per_expt(d_spec, expts):
    """
    Calculate the number of models which have a given variable in a set of specfied experiments
    Calculate the associated data volume

    :return:
    """

    ############################################################################################################
    # GENERATE A LIST OF MODELS FOR A GROUP OF EXPERIMENTS WHERE A GIVEN VARIABLE EXISTS IN ALL THE EXPERIMENTS
    # RESULT IS STORED IN A DICTIONARY
    models_by_experiment = {}
    for experiment in expts:
        datasets = Dataset.objects.filter(variable=d_spec.variable, cmor_table=d_spec.cmor_table,
                                          frequency=d_spec.frequency, experiment=experiment)
        models = set([])
        for dataset in datasets.all():
            models.add(dataset.model)
        models_by_experiment[experiment] = list(models)

    valid_models = check_in_all_models(d_spec, models_by_experiment)
    # print valid_models
    ############################################################################################################


    ############################################################################################################
    # CALCULATE DATA VOLUMES FOR ALL VALID MODELS
    d_spec.data_volume = 0.0
    volume = 0.0
    # Dataset.objects.all().prefetch_related('')
    for model in valid_models:
        for experiment in expts:
            ds = Dataset.objects.filter(variable=d_spec.variable, cmor_table=d_spec.cmor_table,
                                        frequency=d_spec.frequency, experiment=experiment, model=model)
            for d in ds.all():
                volume += d.datafile_set.all().aggregate(Sum('size')).values()[0]
    d_spec.data_volume = volume / (1024. ** 3)
    #    print d_spec.variable, d_spec.frequency, d_spec.data_volume, len(valid_models)
    d_spec.save()
    ############################################################################################################
    # return valid_models


def check_in_all_models(d_spec, models_per_experiment):
    """
    Perform an intersection check to determine
    what list of models all variables are in over a
    dictionary of experiments and with list of models
    """

    in_all = None
    num_models = 0
    for key, models in models_per_experiment.iteritems():
        if in_all is None:
            in_all = set(models)
            num_models = len(in_all)
        else:
            in_all.intersection_update(models)
            num_models = len(in_all)
    d_spec.number_of_models = num_models
    d_spec.save()

    if not in_all:
        return []
    else:
        return list(in_all)

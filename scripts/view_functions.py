import django
django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

import collections, os, timeit, datetime, time, re, glob
import commands
import requests, itertools
from tqdm import tqdm



def get_models_availability(variables, table, frequency, experiment):
    """

    This function will search for a list of simulations where a simulation is defined as:
    simulation = institute + model for a given experiment and will return this list of
    simultions in which all the variables specified are presnet
    together with a list of ensemble members for the simulation.

    :param variables: A list of variables to be searched on   
    :param table: A list of tables corresponding to the variables
    :param frequency: A list of frequencies corresponding to the variables
    :param experiment: A single experiment in which to search

    :return: Dictionary object where the model is key and the values are a list of ensemble members
    """
    
    if experiment not in ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']:
        raise Exception("The value for experiment is not valid, it must be one of 'historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85'")

    for input_variable in [variables, table, frequency]:

        if not isinstance(input_variable, list):
            input_variable = list(input_variable)

    model = Dataset.objects.filter(experiment=experiment)

    all_models = []
    model_ensmeble_dict = {}

    for v, t, f in zip(variables, table, frequency):
        models = model.filter(variable=v, cmor_table=t, frequency=f)
        distinct_models = models.distinct('model')
        all_models.append(distinct_models)

    unique_models = set()
    for m in all_models:
        for mod in m:
            unique_models.add(mod.model)

    for m in unique_models:
        ensembles = models.filter(model=m)
        distinct_ensembles = ensembles.distinct('ensemble')
        if len(distinct_ensembles) > 2:
            model_ensmeble_dict[m] = [e.ensemble for e in distinct_ensembles]

    return model_ensmeble_dict


def list_intersect(list):
    return [[x for x in list[0] if x in list[1]]]








def cf_error_list():

    check = "CF"
    cferrs = QCerror.objects.filter(check_type=check).values_list('error_msg', flat=True)

    cf_errors = {}
    CF = set(cferrs)

    for e in CF:
        qc = {}
        dfs_cc = DataFile.objects.filter(qcerror__error_msg=e)
        # dfs_cc = dfs_cc.filter(dataset__model="HadGEM2-ES")
        for df in dfs_cc:
            _ = df.qcerror_set.filter(check_type=check, error_msg=e).first()
            qc[df.archive_path] = _.report_filepath.replace("group_workspaces/jasmin2/cp4cds1/qc/qc-app2/", "qcapp/cp4cds_gws_qc/")
        cf_errors[e] = qc

    return cf_errors


def cedacc_error_list():

    ccerrs = QCerror.objects.filter(check_type='CEDA-CC').values_list('error_msg', flat=True)

    ceda_cc_errors = {}
    CC = set(ccerrs)

    for e in CC:
        qc = {}
        dfs_cc = DataFile.objects.filter(qcerror__error_msg=e)
        # dfs_cc = dfs_cc.filter(dataset__model="HadGEM2-ES")
        for df in dfs_cc:
            _ = df.qcerror_set.filter(check_type='CEDA-CC', error_msg=e).first()
            qc[df.archive_path] = _.report_filepath.replace("group_workspaces/jasmin2/cp4cds1/qc/qc-app2/", "qcapp/cp4cds_gws_qc/")
        ceda_cc_errors[e] = qc

    return ceda_cc_errors



def max_timeseries_qc_errors(ts):
    """
    Input is of the format of a dictionary of dictonary e.g.
    {'filename': {'global': 0, 'variable': 1, 'other', 1}}

    COPIED FROM qc_functions.py

    :param ts:
    :return:
    """

    max_errors = {'global': 0, 'variable': 0, 'other': 0}

    for key in ['global', 'variable', 'other']:
        errors = []
        for file, errs in ts.iteritems():
            errors.append(errs[key])
        max_errors[key] = max(errors)

    return max_errors


def get_total_qc_errors(qcfile):
    """

    COPIED FROM qc_functions.py

    """
    files = DataFile.objects.filter(ncfile=qcfile)
    # if files != 1:
    #    raise Exception("Length of files %s must not be greater than 1, length is %s: " % (qcfile, len(files)))

    file = files.first()
    qc_errors = file.qcerror_set.all()
    errors = {}
    errors['global'] = qc_errors.filter(error_type='global').count()
    errors['variable'] = qc_errors.filter(error_type='variable').exclude(error_msg__contains="ERROR (4)").count()
    errors['other'] = qc_errors.filter(error_type='other').exclude(error_msg__contains="ERROR (4)").count()

    return errors


def get_list_of_qc_files():
    """

    COPIED FROM qc_functions.py

    """
    for dataset in Dataset.objects.all():
        datafiles = dataset.datafile_set.all()
        for dfile in datafiles:
            qc_errors = dfile.qcerror_set.all()
            for error in qc_errors:
                path = error.file.archive_path
                file = error.file.ncfile





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

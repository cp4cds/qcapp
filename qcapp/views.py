from django.shortcuts import render
from django.http import HttpResponse

from qcapp.models import *
from .models import *
from data_availability_functions import *
from esgf_search_functions import *
from qc_functions import *
from timeseries_and_md5s import *

import os, collections


def documentation(request):

    return render(request, 'qcapp/documentation.html', {'page_title': 'Documentation'})


def data_spec_model(request):

    dataSpec = DataSpecification.objects.filter(datarequesters__requested_by__contains='CP4CDS')
    return render(request, 'qcapp/data-spec-model.html', {'dataSpec': dataSpec})


def data_spec(request):


    exptsSelected = request.GET.keys()
    dataSpec = DataSpecification.objects.filter(datarequesters__requested_by__contains='CP4CDS')
    for spec in dataSpec:
        get_no_models_per_expt(spec, exptsSelected)

    return render(request, 'qcapp/data-spec.html', {'page_title': "Data requested", 'dataSpec': dataSpec, 'expts': exptsSelected})
#    dataSpec = DataSpecification.objects.all()
#    return  render(request, 'qcapp/data-spec.html', {'page_title': "Data requested by CP4CDS", 'dataSpec': dataSpec} )


def variable_summary(request, variable):

    dataset = Dataset.objects.filter(variable=variable)
    return render(request, 'qcapp/variable-summary.html', {'dataset': dataset})

def var_qcplot(request, variable):

    dataset = Dataset.objects.filter(variable=variable)
    return render(request, 'qcapp/var-qcplot.html', {'page_title': 'Variable quality control plot','dataset': dataset})


#def dataset_qc(request, variable):


#    inst = set([])
#    model = set([])
#    expt = set([])
#    freq = set([])
#    realm = set([])
#    table = set([])
#    ens = set([])

#    for i in Dataset.objects.all():
#        inst.add(i.institute)
#        model.add(i.institute)
#        expt.add(i.institute)
#        freq.add(i.institute)
#        realm.add(i.institute)
#        table.add(i.institute)
#        ens.add(i.institute)

#    file = DataFile.objects.first()
    #    file = DataFile.objects.filter(institute=ins, model=model, experiment=expt, frequency=freq, realm=realm,
    #                                   cmor_table=table, ensemble=ens)
#    cf_qc = file.qccheck_set.filter(qc_check_type='CF')
#    cedacc_qc = file.qccheck_set.filter(qc_check_type='CEDA-CC')

#    ds_id = os.path.dirname(file.filepath).replace('/', '.')[1:]
#    filen = os.path.basename(file.filepath)
#    title = "Dataset: %s File: %s" % (ds_id, filen)

#    return render(request, 'qcapp/file-qc.html',
#                  {'page_title': title, 'cf_qc': cf_qc, 'cedacc_qc': cedacc_qc})
    #                   'inst': inst, 'model': model, 'expt': freq, 'realm': realm, 'table': table, 'ens': ens}
    #                  )


 #   dataset = Dataset.objects.filter(variable=variable)
 #   return render(request, 'qcapp/var-qcplot.html', {'page_title': 'Variable quality control plot', 'dataset': dataset, })



#def variable_qc(request):

#    variable = 'tas'
#    model = 'FGOALS-g2'
#    files = DataFile.objects.filter(variable=variable, dataset__model=model)
#    first = files[0]
#    qc_errors = first.qcerror_set.all()

#    global_errs = qc_errors.filter(error_type='global')
#    var_errs = qc_errors.filter(error_type='variable')
#    other_errs = qc_errors.filter(error_type='other')
#
#    qc_errs = {'global': global_errs.count(), 'variable': var_errs.count(), 'other': other_errs.count()}


#    filepath = os.path.join( "/group_workspaces/jasmin/cp4cds1/qc/QCchecks/CEDACC-OUTPUT/LASG-CESS/FGOALS-g2/historical/Amon/v1/",
#                             qc_errors.first().report_filepath)
#    filepath = ''
#    filename = first.archive_path
#    ds_id = first.dataset.dataset_id
#    #ds_id = os.path.dirname(file.filepath).replace('/', '.')[1:]
    #filen = os.path.basename(file.filepath)
#    title = "Variable %s dataset: \n %s \n %s" % (variable, ds_id, os.path.basename(filename))

#    print qc_errors.get_qc_report()

#    return render(request, 'qcapp/variable-qc.html',
#                  {'page_title': title, 'qc_errs': qc_errs, 'filepath': filepath, 'qc_errors': qc_errors})


def dataset_qc(request, variable):


    title = "Dataset QC: %s" % variable
    facets = collections.OrderedDict()
    facets['institutes'] = list(Dataset.objects.values_list('institute').distinct())
    facets['models'] = list(Dataset.objects.values_list('model').distinct())
    facets['frequencies'] = list(Dataset.objects.values_list('frequency').distinct())
    facets['realms'] = list(Dataset.objects.values_list('realm').distinct())
    facets['tables'] = list(Dataset.objects.values_list('cmor_table').distinct())
    facets['ensembles'] = list(Dataset.objects.values_list('ensemble').distinct())

    return render(request, 'qcapp/dataset-qc.html',
                  {'page_title': title, 'facets': facets})


def file_qc(request, version, ncfile):

    files = DataFile.objects.filter(ncfile=ncfile, dataset__version=version)
    file = files.first()
    qc_errors = file.qcerror_set.all()

    global_errs = qc_errors.filter(error_type='global')
    var_errs = qc_errors.filter(error_type='variable')
    other_errs = qc_errors.filter(error_type='other')
    qc_error_counts = {'global': global_errs.count(), 'variable': var_errs.count(), 'other': other_errs.count()}

    #    filepath = os.path.join( "/group_workspaces/jasmin/cp4cds1/qc/QCchecks/CEDACC-OUTPUT/LASG-CESS/FGOALS-g2/historical/Amon/v1/",
    #                             qc_errors.first().report_filepath)

    # ds_id = os.path.dirname(file.filepath).replace('/', '.')[1:]
    # filen = os.path.basename(file.filepath)
    # title = "Variable %s dataset: \n %s \n %s" % (variable, ds_id, os.path.basename(filename))
    title = "Variable quality control summary"
    dataset_id = file.dataset.dataset_id
    filename = os.path.basename(file.archive_path)

    return render(request, 'qcapp/file-qc.html', {'page_title': title, 'dataset_id': dataset_id, 'filename': filename,
                                                  'qc_errors': qc_errors, 'qc_error_counts': qc_error_counts})


def variable_timeseries_qc(request):

    timeseries_datafiles = {}
    for dataset in Dataset.objects.all():
        timeseries_datafiles[Dataset.dataset_id] = dataset.datafile_set.filter(variable='tas', timeseries=True)

    title = "Variable timeseries QC information"

    return render(request, 'qcapp/variable-timeseries-qc.html',
                  {'page_title': title, 'timeseries_datafiles': timeseries_datafiles})




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


def variable_dataset_qc(request, variable):


    title = "Dataset QC: %s" % variable
    facets = collections.OrderedDict()
    facets['institutes'] = list(Dataset.objects.values_list('institute').distinct())
    facets['models'] = list(Dataset.objects.values_list('model').distinct())
    facets['frequencies'] = list(Dataset.objects.values_list('frequency').distinct())
    facets['realms'] = list(Dataset.objects.values_list('realm').distinct())
    facets['tables'] = list(Dataset.objects.values_list('cmor_table').distinct())
    facets['ensembles'] = list(Dataset.objects.values_list('ensemble').distinct())

    error_files = ["zg_Amon_MPI-ESM-LR_rcp85_r1i1p1_220001-220912.nc",
                   "zg_Amon_MPI-ESM-LR_piControl_r1i1p1_230001-230912.nc",
                   "tsice_OImon_inmcm4_historical_r1i1p1_185001-200512.nc"]
    return render(request, 'qcapp/variable-dataset-qc.html',
                  {'page_title': title, 'facets': facets, 'error_files': error_files})


def get_total_qc_errors(qcfile):
    files = DataFile.objects.filter(ncfile=qcfile)
    if files > 1:
        "ERROR"

    file = files.first()
    qc_errors = file.qcerror_set.all()
    errors = {}
    errors['global'] = qc_errors.filter(error_type='global').count()
    errors['variable'] = qc_errors.filter(error_type='variable').count()
    errors['other'] = qc_errors.filter(error_type='other').count()

    return errors

def file_qc(request, ncfile):

#    files = DataFile.objects.filter(ncfile=ncfile, dataset__version=version)
    files = DataFile.objects.filter(ncfile=ncfile)
    file = files.first()
    qc_errors = file.qcerror_set.all()
    qc_cf_errors = file.qcerror_set.filter(check_type='CF')
    qc_cedacc_errors = file.qcerror_set.filter(check_type='CEDA-CC')

    global_cf_errs = qc_cf_errors.filter(error_type='global')
    var_cf_errs = qc_cf_errors.filter(error_type='variable')
    other_cf_errs = qc_cf_errors.filter(error_type='other')

    global_cedacc_errs = qc_cedacc_errors.filter(error_type='global')
    var_cedacc_errs = qc_cedacc_errors.filter(error_type='variable')
    other_cedacc_errs = qc_cedacc_errors.filter(error_type='other')

    global_total_errs = qc_errors.filter(error_type='global')
    var_total_errs = qc_errors.filter(error_type='variable')
    other_total_errs = qc_errors.filter(error_type='other')

    qc_error_counts = {'cf_global': global_cf_errs.count(), 'cf_variable': var_cf_errs.count(), 'cf_other': other_cf_errs.count(),
                       'cedacc_global': global_cedacc_errs.count(), 'cedacc_variable': var_cedacc_errs.count(), 'cedacc_other': other_cedacc_errs.count(),
                       'global': global_total_errs.count(), 'variable': var_total_errs.count(), 'other': other_total_errs.count()}

    #    filepath = os.path.join( "/group_workspaces/jasmin/cp4cds1/qc/QCchecks/CEDACC-OUTPUT/LASG-CESS/FGOALS-g2/historical/Amon/v1/",
    #                             qc_errors.first().report_filepath)

    # ds_id = os.path.dirname(file.filepath).replace('/', '.')[1:]
    # filen = os.path.basename(file.filepath)
    # title = "Variable %s dataset: \n %s \n %s" % (variable, ds_id, os.path.basename(filename))
    title = "Variable quality control summary"
    dataset_id = file.dataset.dataset_id
    filename = os.path.basename(file.archive_path)

    return render(request, 'qcapp/file-qc.html', {'page_title': title, 'dataset_id': dataset_id, 'filename': filename,
                                                  'qc_cf_errors': qc_cf_errors, 'qc_cedacc_errors': qc_cedacc_errors,
                                                  'qc_error_counts': qc_error_counts})


def variable_timeseries_qc(request):

    title = "Variable timeseries QC information"

    ncfile = 'tos_Omon_GFDL-ESM2M_rcp45_r1i1p1_201601-202012.nc'
    ncfile = 'tsice_OImon_inmcm4_historical_r1i1p1_185001-200512.nc'
#    ncfile = 'zos_Omon_ACCESS1-3_rcp45_r1i1p1_200601-210012.nc'
    var, table, model, expt, ens = ncfile.split('_')[:-1]
    timeseries_df_errors = {}

    for datafile in DataFile.objects.filter(dataset__variable=var, dataset__cmor_table=table, dataset__model=model,
                                            dataset__experiment=expt, dataset__ensemble=ens, timeseries=True):
        timeseries_df_errors[datafile.ncfile] = get_total_qc_errors(datafile.ncfile)
        total_errors = sum_timeseries_qc_errors(timeseries_df_errors)

    if len(timeseries_df_errors) > 0:
        return render(request, 'qcapp/variable-timeseries-qc.html',
                      {'page_title': title, 'timeseries_df_errors': timeseries_df_errors, 'total_errors': total_errors})
    else:
        files = DataFile.objects.filter(ncfile=ncfile)
        file = files.first()
        qc_errors = file.qcerror_set.all()
        qc_cf_errors = file.qcerror_set.filter(check_type='CF')
        qc_cedacc_errors = file.qcerror_set.filter(check_type='CEDA-CC')

        global_cf_errs = qc_cf_errors.filter(error_type='global')
        var_cf_errs = qc_cf_errors.filter(error_type='variable')
        other_cf_errs = qc_cf_errors.filter(error_type='other')

        global_cedacc_errs = qc_cedacc_errors.filter(error_type='global')
        var_cedacc_errs = qc_cedacc_errors.filter(error_type='variable')
        other_cedacc_errs = qc_cedacc_errors.filter(error_type='other')

        global_total_errs = qc_errors.filter(error_type='global')
        var_total_errs = qc_errors.filter(error_type='variable')
        other_total_errs = qc_errors.filter(error_type='other')

        qc_error_counts = {'cf_global': global_cf_errs.count(), 'cf_variable': var_cf_errs.count(),
                           'cf_other': other_cf_errs.count(),
                           'cedacc_global': global_cedacc_errs.count(), 'cedacc_variable': var_cedacc_errs.count(),
                           'cedacc_other': other_cedacc_errs.count(),
                           'global': global_total_errs.count(), 'variable': var_total_errs.count(),
                           'other': other_total_errs.count()}

        #    filepath = os.path.join( "/group_workspaces/jasmin/cp4cds1/qc/QCchecks/CEDACC-OUTPUT/LASG-CESS/FGOALS-g2/historical/Amon/v1/",
        #                             qc_errors.first().report_filepath)

        # ds_id = os.path.dirname(file.filepath).replace('/', '.')[1:]
        # filen = os.path.basename(file.filepath)
        # title = "Variable %s dataset: \n %s \n %s" % (variable, ds_id, os.path.basename(filename))
        title = "Variable quality control summary"
        dataset_id = file.dataset.dataset_id
        filename = os.path.basename(file.archive_path)

        return render(request, 'qcapp/file-qc.html',
                      {'page_title': title, 'dataset_id': dataset_id, 'filename': filename,
                       'qc_cf_errors': qc_cf_errors, 'qc_cedacc_errors': qc_cedacc_errors,
                       'qc_error_counts': qc_error_counts})



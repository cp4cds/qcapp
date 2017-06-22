from django.shortcuts import render
from django.http import HttpResponse

from qcapp.models import *
from .models import *
from data_availability_functions import *
from esgf_search_functions import *
from qc_functions import *
from timeseries_and_md5s import *

import os, collections, json


def documentation(request):

    return render(request, 'qcapp/documentation.html', {'page_title': 'Documentation'})

def data_spec(request):

    project = "CP4CDS"
    exptsSelected = request.GET.keys()
    dataSpec = DataSpecification.objects.filter(datarequesters__requested_by__contains=project)
    for spec in dataSpec:
        get_no_models_per_expt(spec, exptsSelected)
    title = "Data Requested by %s" % project

    return render(request, 'qcapp/data-spec.html', {'page_title': title, 'dataSpec': dataSpec, 'expts': exptsSelected})


def variable_summary_qc(request):

    project = "CP4CDS"
    dataSpec = DataSpecification.objects.filter(datarequesters__requested_by__contains=project)
    title = "Quality control information for  %s" % project

    return render(request, 'qcapp/variable-summary-qc.html', {'page_title': title, 'dataSpec': dataSpec})



def variable_dataset_qc(request, variable):


    title = variable
    facets = collections.OrderedDict()
    facets['institutes'] = [str(x[0]).strip() for x in Dataset.objects.values_list('institute').distinct()]
    facets['models'] = [str(x[0]).strip() for x in Dataset.objects.values_list('model').distinct()]
    facets['experiments'] = [str(x[0]).strip() for x in Dataset.objects.values_list('experiment').distinct()]
    facets['frequencies'] = [str(x[0]).strip() for x in Dataset.objects.values_list('frequency').distinct()]
    facets['realms'] = [str(x[0]).strip() for x in Dataset.objects.values_list('realm').distinct()]
    facets['tables'] = [str(x[0]).strip() for x in Dataset.objects.values_list('cmor_table').distinct()]
    facets['ensembles'] = [str(x[0]).strip() for x in Dataset.objects.values_list('ensemble').distinct()]

    errors = []
    if request.POST:
        for ds in Dataset.objects.filter(variable=variable):
            for df in ds.datafile_set.all():
                for error in df.qcerror_set.all().exclude(error_msg__contains="ERROR (4)"):
                    errors.append(error)


#    error_files = ["zg_Amon_MPI-ESM-LR_rcp85_r1i1p1_220001-220912.nc",
#                   "zg_Amon_MPI-ESM-LR_piControl_r1i1p1_230001-230912.nc",
#                   "tsice_OImon_inmcm4_historical_r1i1p1_185001-200512.nc",
#                   "zos_Omon_ACCESS1-3_rcp45_r1i1p1_200601-210012.nc"]
    return render(request, 'qcapp/variable-dataset-qc.html',
                  {'page_title': title, 'facets': facets, 'errors': errors})


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

def variable_file_qc(request, ncfile):

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

    return render(request, 'qcapp/variable-file-qc.html', {'page_title': title, 'dataset_id': dataset_id, 'filename': filename,
                                                  'qc_cf_errors': qc_cf_errors, 'qc_cedacc_errors': qc_cedacc_errors,
                                                  'qc_error_counts': qc_error_counts})


def variable_timeseries_qc(request):

    title = "Variable timeseries QC information"

    ncfile = 'tos_Omon_GFDL-ESM2M_rcp45_r1i1p1_201601-202012.nc'
#    ncfile = 'tsice_OImon_inmcm4_historical_r1i1p1_185001-200512.nc'
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

        return render(request, 'qcapp/variable-file-qc.html',
                      {'page_title': title, 'dataset_id': dataset_id, 'filename': filename,
                       'qc_cf_errors': qc_cf_errors, 'qc_cedacc_errors': qc_cedacc_errors,
                       'qc_error_counts': qc_error_counts})


def facet_filter(request, institutes, models, experiments, frequencies, realms, tables, ensembles):

    if request.is_ajax():
        print institutes, models, experiments, frequencies, realms, tables, ensembles
        facets = collections.OrderedDict()


        all_facets = Dataset.objects.all()
        if institutes != 'All':
            all_facets.filter(institute=institutes)
        if models != 'All':
            all_facets.filter(model=models)
        if experiments != 'All':
            all_facets.filter(experiment=experiments)
        if frequencies != 'All':
            all_facets.filter(frequency=frequencies)
        if realms != 'All':
            all_facets.filter(realm=realms)
        if tables != 'All':
            all_facets.filter(cmor_table=tables)
        if ensembles != 'All':
            all_facets.filter(ensemble=ensembles)
        # THIS IS NOT BEHAVING
        print all_facets.values('realm')

        facets['institutes'] = [str(x[0]).strip()
                                for x in all_facets.values_list('institute')]
        facets['models'] = [str(x[0]).strip()
                                for x in all_facets.values_list('model')]
        facets['frequencies'] = [str(x[0]).strip()
                                for x in all_facets.values_list('frequency')]
        facets['realms'] = [str(x[0]).strip()
                                for x in all_facets.values_list('realm')]
        facets['tables'] = [str(x[0]).strip()
                                for x in all_facets.values_list('cmor_table')]
        facets['ensembles'] = [str(x[0]).strip()
                                for x in all_facets.values_list('ensemble')]
        filtered_data = json.dumps(facets)
        print filtered_data#
        return HttpResponse(filtered_data, content_type='application/json')

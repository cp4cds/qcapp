from django.shortcuts import render
from django.http import HttpResponse

from qcapp.models import *
from .models import *
from data_availability_functions import *
from esgf_search_functions import *
from qc_functions import *
from timeseries_and_md5s import *

import os, collections, json

GWSDIR = "qcapp/cp4cds_gws_qc/"

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
    facets['models'] = [str(x[0]).strip() for x in Dataset.objects.values_list('model').distinct()]
    facets['experiments'] = [str(x[0]).strip() for x in Dataset.objects.values_list('experiment').distinct()]
    # facets['institutes'] = [str(x[0]).strip() for x in Dataset.objects.values_list('institute').distinct()]
    # facets['frequencies'] = [str(x[0]).strip() for x in Dataset.objects.values_list('frequency').distinct()]
    # facets['realms'] = [str(x[0]).strip() for x in Dataset.objects.values_list('realm').distinct()]
    # facets['tables'] = [str(x[0]).strip() for x in Dataset.objects.values_list('cmor_table').distinct()]
    # facets['ensembles'] = [str(x[0]).strip() for x in Dataset.objects.values_list('ensemble').distinct()]
    #
    errors = []
    error_files = {}
    timeseries_df_errors = {}
    max_errors = {}
    unique_dataset = set()
    error_info = {}
    if request.POST:
        for ds in Dataset.objects.filter(variable=variable):
            for df in ds.datafile_set.all():
                # ncfile = df.ncfile
                # errors.append(ncfile)
                # var, table, model, expt, ens = ncfile.split('_')[:-1]
                # for datafile in DataFile.objects.filter(dataset__variable=var, dataset__cmor_table=table,
                #                                         dataset__model=model,
                #                                         dataset__experiment=expt, dataset__ensemble=ens,
                #                                         timeseries=True):
                #     timeseries_df_errors[datafile.ncfile] = get_total_qc_errors(datafile.ncfile)
                #     max_errors = max_timeseries_qc_errors(timeseries_df_errors)
                #     print max_errors
                for error in df.qcerror_set.all().exclude(error_msg__contains="ERROR (4)"):
                    ncfile = error.file.ncfile
                    errors.append(error)
                    unique_dataset.add('.'.join(ncfile.split('_')[:-1]))
                    # error_info['.'.join(ncfile.split('_')[:-1])].append(error)
                    # error_info['.'.join(ncfile.split('_')[:-1])] = errors
                    # error_files[error.file.ncfile] = errors
                    # ncfile = error.file.ncfile
                    # var, table, model, expt, ens = ncfile.split('_')[:-1]

                    # for datafile in DataFile.objects.filter(dataset__variable=var, dataset__cmor_table=table, dataset__model=model,
                    #                                         dataset__experiment=expt, dataset__ensemble=ens, timeseries=True):
                    #
                    #     print datafile.ncfile
                    #     timeseries_df_errors[datafile.ncfile] = get_total_qc_errors(datafile.ncfile)
                    #     max_errors = max_timeseries_qc_errors(timeseries_df_errors)


    # for dataset in unique_dataset:
    #     var, table, model, expt, ens = dataset.split('.')



    # for error in errors:
    #     ncfile = error.file.ncfile
        # var, table, model, expt, ens = ncfile.split('_')[:-1]
        # for datafile in DataFile.objects.filter(dataset__variable=var, dataset__cmor_table=table, dataset__model=model,
        #                                         dataset__experiment=expt, dataset__ensemble=ens, timeseries=True):


            # timeseries_df_errors[datafile.ncfile] = get_total_qc_errors(datafile.ncfile)
            # max_errors = max_timeseries_qc_errors(timeseries_df_errors)



    print "I'm done"
    return render(request, 'qcapp/variable-dataset-qc.html',
                  {'page_title': title, 'facets': facets, 'unique_dataset': unique_dataset})

    # error_files = ["zg_Amon_MPI-ESM-LR_rcp85_r1i1p1_220001-220912.nc",
    #                "zg_Amon_MPI-ESM-LR_piControl_r1i1p1_230001-230912.nc",
    #                "tsice_OImon_inmcm4_historical_r1i1p1_185001-200512.nc",
    #                "zos_Omon_ACCESS1-3_rcp45_r1i1p1_200601-210012.nc"]



def variable_qc(request, ncfile):


    if DataFile.objects.filter(ncfile=ncfile).first().timeseries == True:

        title = "Variable timeseries QC information"
        var, table, model, expt, ens = ncfile.split('_')[:-1]
        timeseries_df_errors = {}

        for datafile in DataFile.objects.filter(dataset__variable=var, dataset__cmor_table=table, dataset__model=model,
                                                dataset__experiment=expt, dataset__ensemble=ens, timeseries=True):
            timeseries_df_errors[datafile.ncfile] = get_total_qc_errors(datafile.ncfile)
        max_errors = max_timeseries_qc_errors(timeseries_df_errors)

        return render(request, 'qcapp/variable-timeseries-qc.html',
                      {'page_title': title, 'timeseries_df_errors': timeseries_df_errors,
                       'max_errors': max_errors})

    else:
        print "not timeseries dataset"
        file = DataFile.objects.filter(ncfile=ncfile).first()
        qc_errors = file.qcerror_set.all().exclude(error_msg__contains="ERROR (4)")
        qc_cf_errors = file.qcerror_set.filter(check_type='CF').exclude(error_msg__contains="ERROR (4)")
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

        title = "Variable quality control summary"
        filename = os.path.basename(file.archive_path)
        dataset_id = file.dataset.dataset_id
        dataset_id = dataset_id.replace('1.0', '1-0').replace('1.3', '1-3').replace('1.1', '1-1').\
            replace('1.m', '1-m').replace('CM2.1', 'CM2p1')
        cmip, output, institute, model, expt, freq, realm, table, ens, var, version = dataset_id.split('.')

        version = 'v' + version
        cf_file = os.path.join(GWSDIR, 'CF-OUTPUT', institute, model, expt, table, version,
                               ncfile.replace('.nc', '.cf-log.txt'))
        cc_file = os.path.join(GWSDIR, 'CEDACC-OUTPUT', institute, model, expt, table, version,
                               ncfile.replace('.nc', '__qclog_20170616.txt'))

        return render(request, 'qcapp/variable-file-qc.html',
                      {'page_title': title, 'dataset_id': dataset_id, 'filename': filename,
                       'qc_cf_errors': qc_cf_errors, 'qc_cedacc_errors': qc_cedacc_errors,
                       'qc_error_counts': qc_error_counts, 'cf_file': cf_file, 'cc_file': cc_file})


def variable_file_qc(request, ncfile):

    files = DataFile.objects.filter(ncfile=ncfile)
    file = files.first()
    qc_errors = file.qcerror_set.all().exclude(error_msg__contains="ERROR (4)")
    qc_cf_errors = file.qcerror_set.filter(check_type='CF').exclude(error_msg__contains="ERROR (4)")
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

    title = "Variable quality control summary"
    dataset_id = file.dataset.dataset_id
    filename = os.path.basename(file.archive_path)
    dataset_id = dataset_id.replace('1.0', '1-0').replace('1.3', '1-3').replace('1.1', '1-1').\
        replace('1.m', '1-m').replace('CM2.1', 'CM2p1')
    cmip, output, institute, model, expt, freq, realm, table, ens, var, version = dataset_id.split('.')

    version = 'v' + version
    cf_file = os.path.join(GWSDIR, 'CF-OUTPUT', institute, model, expt, table, version,
                             ncfile.replace('.nc', '.cf-log.txt'))
    cc_file = os.path.join(GWSDIR, 'CEDACC-OUTPUT', institute, model, expt, table, version,
                            ncfile.replace('.nc', '__qclog_20170616.txt'))

    return render(request, 'qcapp/variable-file-qc.html', {'page_title': title, 'dataset_id': dataset_id, 'filename': filename,
                                                  'qc_cf_errors': qc_cf_errors, 'qc_cedacc_errors': qc_cedacc_errors,
                                                  'qc_error_counts': qc_error_counts, 'cf_file': cf_file, 'cc_file': cc_file})



def variable_timeseries_qc(request, ncfile):


    title = "Variable timeseries QC information"
    var, table, model, expt, ens = ncfile.split('_')[:-1]
    timeseries_df_errors = {}

    for datafile in DataFile.objects.filter(dataset__variable=var, dataset__cmor_table=table, dataset__model=model,
                                            dataset__experiment=expt, dataset__ensemble=ens, timeseries=True):
        timeseries_df_errors[datafile.ncfile] = get_total_qc_errors(datafile.ncfile)
        max_errors = max_timeseries_qc_errors(timeseries_df_errors)

    return render(request, 'qcapp/variable-timeseries-qc.html',
                  {'page_title': title, 'timeseries_df_errors': timeseries_df_errors,
                   'max_errors': max_errors})

# def facet_filter(request, institutes, models, experiments, frequencies, realms, tables, ensembles):
def facet_filter(request, models, experiments):

    if request.is_ajax():
        print models, experiments
        facets = collections.OrderedDict()

        all_facets = Dataset.objects.all()
        if models != 'All':
            all_facets.filter(model=models)
        if experiments != 'All':
            all_facets.filter(experiment=experiments)
        # THIS IS NOT BEHAVING
        print all_facets.values('realm')

        facets['models'] = [str(x[0]).strip()
                                for x in all_facets.values_list('model')]
        facets['experiments'] = [str(x[0]).strip()
                                for x in all_facets.values_list('experiment')]
        filtered_data = json.dumps(facets)
        print filtered_data#
        return HttpResponse(filtered_data, content_type='application/json')

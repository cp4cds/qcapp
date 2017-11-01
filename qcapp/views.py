from django.shortcuts import render
from django.http import HttpResponse

from qcapp.models import *
from .models import *
from view_functions import *

import os, collections, json

GWSDIR = "qcapp/cp4cds_gws_qc/"
FACETS_LIST = ['model', 'experiment']



def qcerrors(request):

    title = "QC Errors - details"
    ccerrs = cedacc_error_list()
    
    return render(request, 'qcapp/qcerrors.html', {'page_title': title, 'ccerrs': ccerrs})

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


    # PAGE TITLE
    title = DataFile.objects.filter(variable=variable).first().variable_long_name

    # FACETS FOR FILTERING ON
    facets = collections.OrderedDict()


    for f in FACETS_LIST:
        facets[f] = [str(x[0]).strip() for x in Dataset.objects.values_list(f).distinct()]
    # facets['institutes'] = [str(x[0]).strip() for x in Dataset.objects.values_list('institute').distinct()]
    # facets['frequencies'] = [str(x[0]).strip() for x in Dataset.objects.values_list('frequency').distinct()]
    # facets['realms'] = [str(x[0]).strip() for x in Dataset.objects.values_list('realm').distinct()]
    # facets['tables'] = [str(x[0]).strip() for x in Dataset.objects.values_list('cmor_table').distinct()]
    # facets['ensembles'] = [str(x[0]).strip() for x in Dataset.objects.values_list('ensemble').distinct()]
    #

    # DECLARE DICTIONARIES
    errors = {}
    timeseries_df_errors = {}
    max_errors = {}
    num_dfs = {}
    # SET UP POST REQUEST
    if request.POST:

        all_facets = Dataset.objects.all()
        if request.POST['model'] != 'All':
            all_facets = all_facets.filter(model=request.POST['model'])
        if request.POST['experiment'] != 'All':
            all_facets = all_facets.filter(experiment=request.POST['experiment'])
        for f in FACETS_LIST:
            facets[f] = [str(x[0]).strip() for x in all_facets.values_list(f).distinct()]

        get_errors = Dataset.objects.filter(variable=variable)
        if request.POST['model'] != 'All':
            get_errors = get_errors.filter(model=request.POST['model'])
        if request.POST['experiment'] != 'All':
            get_errors = get_errors.filter(experiment=request.POST['experiment'])

        for ds in get_errors:
            for df in ds.datafile_set.all():
                gbl_err_count = df.qcerror_set.all().exclude(error_msg__contains="ERROR (4)").\
                                                     filter(error_type='variable').count()
                var_err_count = df.qcerror_set.all().exclude(error_msg__contains="ERROR (4)").\
                                                     filter(error_type='variable').count()
                oth_err_count = df.qcerror_set.all().exclude(error_msg__contains="ERROR (4)").\
                                                     filter(error_type='variable').count()

                for error in df.qcerror_set.all().exclude(error_msg__contains="ERROR (4)"):

                    ins = error.file.dataset.institute
                    model = error.file.dataset.model
                    expt = error.file.dataset.experiment
                    freq = error.file.dataset.frequency
                    realm = error.file.dataset.realm
                    table = error.file.dataset.cmor_table
                    ensemble = error.file.dataset.ensemble
                    ds_id = '    '.join([ins, model, expt, freq, realm, table, ensemble])

                    if not ds_id in errors:
                        errors[ds_id] = [error]
                    else:
                        errors[ds_id].append(error)


                    num_dfs = DataFile.objects.filter(dataset__variable=variable, dataset__institute=ins, dataset__model=model,
                                                      dataset__experiment=expt, dataset__frequency=freq, dataset__realm=realm,
                                                      dataset__cmor_table=table, dataset__ensemble=ensemble, timeseries=True).count()
                    if num_dfs == 0:
                        num_dfs = 1
                    errors[ds_id].insert(0, num_dfs)
                    errors[ds_id].insert(1, gbl_err_count)
                    errors[ds_id].insert(2, var_err_count)
                    errors[ds_id].insert(3, oth_err_count)
                    qc_score = max(errors[ds_id][1:4])
                    errors[ds_id].insert(4, qc_score)

                    # timeseries_df_errors = {}
                    # if DataFile.objects.filter(ncfile=ncfile).first().timeseries == True:
                    #
                    #     title = "Variable timeseries QC information"
                    #     var, table, model, expt, ens = ncfile.split('_')[:-1]
                    #     timeseries_df_errors = {}
                    #
                    #     for datafile in DataFile.objects.filter(dataset__variable=variable, dataset__cmor_table=table, dataset__model=model,
                    #                                             dataset__experiment=experiment, dataset__ensemble=ensemble, timeseries=True):
                    #         timeseries_df_errors[datafile.ncfile] = get_total_qc_errors(datafile.ncfile)
                    #
                    #     max_errors = max_timeseries_qc_errors(timeseries_df_errors)
                    #

    return render(request, 'qcapp/variable-dataset-qc.html',
                  {'page_title': title, 'variable': variable, 'facets': facets, 'errors': errors, 'num_dfs': num_dfs})


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

# def facet_filter(request, institutes, model, experiment, frequencies, realms, tables, ensembles):

def facet_filter(request, model, experiment):

    if request.is_ajax():
        facets = collections.OrderedDict()

        all_facets = Dataset.objects.all()
        if model != 'All':
            all_facets = all_facets.filter(model=model)
        if experiment != 'All':
            all_facets = all_facets.filter(experiment=experiment)

        for f in FACETS_LIST:
            facets[f] = [str(x[0]).strip() for x in all_facets.values_list(f).distinct()]

        filtered_data = json.dumps(facets)
        return HttpResponse(filtered_data, content_type='application/json')


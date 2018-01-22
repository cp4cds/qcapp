from django.shortcuts import render
from django.http import HttpResponse
from django.db.models import Q
from django.conf import settings

import datetime

from qcapp.models import *
from .models import *
from view_functions import *
import os, collections, json
import view_functions as vf


GWSDIR = "qcapp/cp4cds_gws_qc/"
FACETS_LIST = ['model', 'experiment']


def home(request):
    title = "CP4CDS Quality Control"
    return render(request,'qcapp/home.html', {'page_title': title, 'version': settings.VERSION})

def help(request):
    context={}
    title = "Help Page"



    context["page_title"] = title
    context["version"] = settings.VERSION
    return render(request,'qcapp/help.html', context)

def cf_errors(request):

    title = "CF Errors: details"
    cf_errs = vf.cf_error_list()

    return render(request, 'qcapp/cf-errors.html', {'page_title': title, 'cf_errs': cf_errs})


def ceda_cc_errors(request):

    title = "CEDA CC Errors: details"
    ccc_errs = vf.cedacc_error_list()

    return render(request, 'qcapp/ceda-cc-errors.html', {'page_title': title, 'ccc_errs': ccc_errs})


def qcerrors(request):

    title = "QC Errors - details"
    ccc_errs = vf.cedacc_error_list()

    return render(request, 'qcapp/qcerrors.html', {'page_title': title, 'ccc_errs': ccc_errs})

def documentation(request):

    return render(request, 'qcapp/documentation.html', {'page_title': 'Documentation'})


def data_spec(request):

    project = "CP4CDS"
    exptsSelected = request.GET.keys()
    dataSpec = DataSpecification.objects.filter(datarequesters__requested_by__contains=project)
    for spec in dataSpec:
        vf.get_no_models_per_expt(spec, exptsSelected)
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
            timeseries_df_errors[datafile.ncfile] = vf.get_total_qc_errors(datafile.ncfile)

        max_errors = vf.max_timeseries_qc_errors(timeseries_df_errors)

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
        timeseries_df_errors[datafile.ncfile] = vf.get_total_qc_errors(datafile.ncfile)
        max_errors = vf.max_timeseries_qc_errors(timeseries_df_errors)

    return render(request, 'qcapp/variable-timeseries-qc.html',
                  {'page_title': title, 'timeseries_df_errors': timeseries_df_errors,
                   'max_errors': max_errors})

# def facet_filter(request, institutes, model, experiment, frequencies, realms, tables, ensembles):


def model_details(request):
    context = {}

    context["page_title"] = "Model Details"


    if request.POST:

        context["selected"] = {"model":request.POST["model"], "experiment": request.POST["experiment"]}

        datasets = Dataset.objects.all()

        if request.POST['model'] != "All":
            datasets = datasets.filter(model=request.POST['model'])

        if request.POST['experiment'] != "All":
            datasets = datasets.filter(experiment=request.POST['experiment'])

        variables = datasets.values_list('variable', flat=True).distinct().order_by('variable')

        vars = []
        for var in variables:
            data = {}
            expr_list = []
            experiments = datasets.filter(variable=var).values_list('experiment', flat=True).distinct()
            for expr in experiments:
                experiment_data = {}

                ensembles = list(datasets.filter(experiment=expr).values_list('ensemble', flat=True).distinct().order_by('ensemble'))

                experiment_data["experiment"] = expr
                experiment_data["ensembles"] = ensembles
                expr_list.append(experiment_data)

            data['experiments'] = expr_list
            data['var_name'] = var
            data['var_long_name'] = DataFile.objects.filter(variable=var).first().variable_long_name
            vars.append(data)

        context["variables"] = vars

    context["models"] = Dataset.objects.values_list("model", flat=True).distinct().order_by('model')
    context["experiments"] = Dataset.objects.values_list("experiment", flat=True).distinct().order_by('experiment')
    context["version"] = settings.VERSION


    return render(request, 'qcapp/model_details.html', context)

def variable_details(request):
    context = {}

    context["page_title"] = "Variable Details"

    if request.POST:

        datasets = Dataset.objects.all()

        if request.POST["variable"] != "All":
            datasets = datasets.filter(variable=request.POST["variable"])
        if request.POST["table"] != "All":
            datasets = datasets.filter(cmor_table=request.POST["table"])
        if request.POST["frequency"] != "All":
            datasets = datasets.filter(frequency=request.POST["frequency"])

        models = datasets.values_list('model', flat=True).distinct().order_by('institute')

        model_data = []
        for model in models:
            data = {}
            model_datasets = datasets.filter(model=model)
            ensembles = list(model_datasets.values_list('ensemble', flat=True).distinct().order_by('ensemble'))
            data['model'] = model
            data['ensembles'] = ensembles
            data['institute'] = model_datasets.first().institute
            model_data.append(data)

        context["models"] = model_data
        context["selected"] = {"variable":request.POST["variable"],"table":request.POST["table"],"frequency":request.POST["frequency"]}

    context["variables"] = Dataset.objects.values_list("variable", flat=True).distinct().order_by('variable')
    context["cmor_tables"] = Dataset.objects.values_list("cmor_table", flat=True).distinct()
    context["frequencies"] = Dataset.objects.values_list("frequency", flat=True).distinct()
    context["version"] = settings.VERSION

    return render(request, 'qcapp/variable_details.html', context)



def data_availability_matrix(request):
    context = {}
    context["page_title"] = "Data Availability Matrix"

    if request.POST and request.is_ajax():
        data = dict(request.POST.lists())
        # Remove csrf token before sending query back in JSON.
        data.pop('csrfmiddlewaretoken', None)
        try:
            variables = data["variables"]
            tables = data['tables']
            freqs = data['frequencies']
            experiments = data['experiments']
            min_size = int(data["ensemble_size"][0])

        except KeyError:
            # User has not made a selection for one of the filters. Return bad request code.
            return HttpResponse(status=400)

        # Get datasets that have one or other of the experiments
        datasets = Dataset.objects.filter(experiment__in=experiments)

        output = []
        for v,t,f in zip(variables, tables, freqs):
            output.append(list(datasets.filter(variable=v,cmor_table=t,frequency=f).distinct('model').values_list('model', flat=True)))
            if len(output) > 1:
                output = vf.list_intersect(output)

        models = output[0]

        return_data_list = []
        for model in models:
            model_specific_datasets = datasets.filter(model=model)
            institute = model_specific_datasets.values_list('institute', flat=True).distinct().first()
            expts = model_specific_datasets.values_list('experiment', flat=True).distinct()
            for experiment in expts:
                model_data = {}
                ensembles = list(model_specific_datasets.filter(experiment=experiment).values_list('ensemble',flat=True).distinct().order_by('ensemble'))
                if len(ensembles) < min_size:
                    continue
                model_data["institute"] = institute
                model_data["model"] = model
                model_data["experiment"] = experiment
                model_data["ensembles"] = ensembles

                return_data_list.append(model_data)

        # Generate Meta data

        provenance = {}
        provenance["source"] = "This data has been provided by the ECMWF C3S Copernicus project Climate Predicitions for the Copernicus Climate Data Store (CP4CDS) provided by CEDA-STFC (c) ECWMF C3S"
        provenance["access_date"] = datetime.datetime.today().strftime('%Y-%M-%d %H:%M:%S')
        provenance["version"] = "CP4CDS QA database version: {}".format(settings.VERSION)

        data_availability = {}
        data_availability["provenance"] = provenance
        data_availability["query"] = data
        data_availability["results"] = return_data_list

        return HttpResponse(json.dumps(data_availability), content_type='application/json')




    datasets = Dataset.objects.all()

    context["range"] = range(1,16)
    context["variables"] = datasets.values_list('variable', flat=True).distinct().order_by('variable')
    context["experiments"] = datasets.values_list('experiment', flat=True).distinct()
    context["version"] = settings.VERSION

    return render(request, 'qcapp/data-availability.html', context)




################################################## AJAX VIEWS #########################################################

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

def get_variable_details(request, variable, table=None, freq=None):
    data = {}
    if request.is_ajax():

        datasets = Dataset.objects.all()
        if variable != 'All':
            datasets = datasets.filter(variable=variable)
        if table != 'All':
            datasets = datasets.filter(cmor_table=table)
        if freq != 'All':
            datasets = datasets.filter(frequency=freq)

        # Make sure only valid pairs are returned
        pairs = list(datasets.values_list('cmor_table','frequency').distinct())
        tables, freqs = [], []
        for table, freq in pairs:
            tables.append(table)
            freqs.append(freq)

        data['variable'] = list(datasets.values_list('variable', flat=True).distinct().order_by('variable'))
        data['tables'] = tables
        data['frequencies'] = freqs

    return HttpResponse(json.dumps(data), content_type='application/json')

################################################## AJAX VIEWS #########################################################
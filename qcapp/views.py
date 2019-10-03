from django.shortcuts import render
from django.http import HttpResponse
from django.conf import settings

import datetime

from qcapp.models import *
from .models import *
import collections, json
import qcapp.utils as apputils


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
                output = apputils.list_intersect(output)

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
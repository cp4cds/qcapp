from django.http import JsonResponse, HttpResponseBadRequest
from django.conf import settings
from django.views.generic import TemplateView

import datetime

from qcapp.models import *
from .models import *
import collections
import qcapp.utils as apputils

FACETS_LIST = ['model', 'experiment']


class HomeView(TemplateView):

    template_name = 'qcapp/home.html'
    extra_context = {
        'page_title': 'CP4CDS Quality Control',
        'version': settings.VERSION
    }


class HelpView(TemplateView):

    template_name = 'qcapp/help.html'
    extra_context = {
        'page_title': 'Help Page',
        'version': settings.VERSION
    }


class ModelDetailView(TemplateView):

    template_name = 'qcapp/model_details.html'
    extra_context = {
        'page_title': 'Model Details',
        'version': settings.VERSION
    }

    def post(self, request, *args, **kwargs):
        context = self.get_context_data(**kwargs)
        context["selected"] = {
            "model": request.POST["model"],
            "experiment": request.POST["experiment"]
        }

        datasets = Dataset.objects.all()

        if request.POST['model'] != "All":
            datasets = datasets.filter(model=request.POST['model'])

        if request.POST['experiment'] != "All":
            datasets = datasets.filter(experiment=request.POST['experiment'])

        variables = datasets.values_list('variable', flat=True).distinct().order_by('variable')

        vars = []
        for var in variables:
            expr_list = []
            experiments = datasets.filter(variable=var).values_list('experiment', flat=True).distinct()

            for expr in experiments:
                ensembles = list(
                    datasets.filter(experiment=expr).values_list('ensemble', flat=True).distinct().order_by('ensemble'))

                experiment_data = {
                    'experiment': expr,
                    'ensembles': ensembles
                }
                expr_list.append(experiment_data)

            data = {
                'experiments': expr_list,
                'var_name': var,
                'var_long_name': apputils.VariableLongName.get_longname(var)
            }

            vars.append(data)

        context["variables"] = vars

        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        datasets = Dataset.objects.all()

        context["models"] = datasets.values_list("model", flat=True).distinct().order_by('model')
        context["experiments"] = datasets.values_list("experiment", flat=True).distinct().order_by('experiment')
        return context


class VariableDetailView(TemplateView):

    template_name = 'qcapp/variable_details.html'
    extra_context = {
        'page_title': 'Variable Details',
        'version': settings.VERSION
    }

    def post(self, request, *args, **kwargs):
        context = self.get_context_data(**kwargs)

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
        context["selected"] = {
            "variable": request.POST["variable"],
            "table": request.POST["table"],
            "frequency": request.POST["frequency"]
        }

        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        datasets = Dataset.objects.all()

        variables = apputils.VariableLongName.short_to_long(
            datasets.values_list("variable", flat=True).distinct().order_by('variable')
        )

        context['variables'] = variables
        context['cmor_tables'] = datasets.values_list("cmor_table", flat=True).distinct()
        context['frequencies'] = datasets.values_list("frequency", flat=True).distinct()

        return context


class DataAvilabilityMatrix(TemplateView):
    template_name = 'qcapp/data-availability.html'
    extra_context = {
        'page_title': 'Data Availability Matrix',
        'version': settings.VERSION
    }

    def post(self, request, *args, **kwargs):
        if not request.is_ajax:
            context = self.get_context_data(**kwargs)
            return self.render_to_response(context)

        # AJAX Response
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
            return HttpResponseBadRequest()

        # Get datasets that have one or other of the experiments
        datasets = Dataset.objects.filter(experiment__in=experiments)

        output = []
        for v, t, f in zip(variables, tables, freqs):
            output.append(
                list(datasets.filter(variable=v, cmor_table=t, frequency=f).values_list('model', flat=True).distinct()))

            if len(output) > 1:
                output = apputils.list_intersect(output)

        models = output[0]

        return_data_list = []
        for model in models:
            model_specific_datasets = datasets.filter(model=model)
            institute = model_specific_datasets.values_list('institute', flat=True).distinct().first()
            expts = model_specific_datasets.values_list('experiment', flat=True).distinct()

            for experiment in expts:
                ensembles = list(
                    model_specific_datasets.filter(experiment=experiment).values_list(
                        'ensemble', flat=True).distinct().order_by(
                        'ensemble')
                )

                if len(ensembles) < min_size:
                    continue

                model_data = {
                    'institute': institute,
                    'model': model,
                    'experiment': experiment,
                    'ensembles': ensembles
                }

                return_data_list.append(model_data)

        # Generate Meta data
        provenance = {
            'source': 'This data has been provided by the ECMWF C3S Copernicus project Climate Predicitions '
                      'for the Copernicus Climate Data Store (CP4CDS) provided by CEDA-STFC (c) ECWMF C3S',
            'access_date': datetime.datetime.today().strftime('%Y-%M-%d %H:%M:%S'),
            'version': f"CP4CDS QA database version: {settings.VERSION}"
        }

        data_availability = {
            'provenance': provenance,
            'query': data,
            'results': return_data_list
        }

        return JsonResponse(data_availability)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        datasets = Dataset.objects.all()

        variables = apputils.VariableLongName.short_to_long(
            datasets.values_list("variable", flat=True).distinct().order_by('variable')
        )

        context["range"] = range(1, 16)
        context["variables"] = variables
        context["experiments"] = datasets.values_list('experiment', flat=True).distinct()

        return context


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

        return JsonResponse(facets)


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
        pairs = list(datasets.values_list('cmor_table', 'frequency').distinct())
        tables, freqs = [], []
        for table, freq in pairs:
            tables.append(table)
            freqs.append(freq)

        data['variable'] = list(datasets.values_list('variable', flat=True).distinct().order_by('variable'))
        data['tables'] = tables
        data['frequencies'] = freqs

    return JsonResponse(data)

################################################## AJAX VIEWS #########################################################

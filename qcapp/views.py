from django.shortcuts import render
from django.http import HttpResponse

from qcapp.models import *
from .models import *
from .app_data_generator import *


# Create your views here.
def happy(request, expt):
    ds = Dataset.objects.filter(experiment=expt).first()

    return HttpResponse("We found a Dataset with these attributes: %s, %s" % (ds.institute, ds.experiment))

def test(request):
    requestName = Request.objects.first().request_name
    return HttpResponse("I am a test." + requestName )

def home(request):

    return render(request, 'qcapp/home.html', {'page_title': 'About CP4CDS: Climate Projections for the Climate Data Store'})

def documentation(request):

    return render(request, 'qcapp/documentation.html', {'page_title': 'Documentation'})



def data_spec_model(request):

    dataSpec = DataSpecification.objects.all()

    return render(request, 'qcapp/data-spec-model.html', {'dataSpec': dataSpec})


def data_spec_experiment(request):

    dataSpec = DataSpecification.objects.all()

    return render(request, 'qcapp/data-spec-experiment.html', {'dataSpec': dataSpec})



def data_spec(request):

#    dataSpec = DataSpecification.objects.filter(variable='tas').first()
    exptsSelected = request.GET.keys()
    dataSpec = DataSpecification.objects.all()
    for spec in dataSpec:
        get_no_models_per_expt(spec, exptsSelected)

    return render(request, 'qcapp/data-spec.html', {'page_title': "Data requested by CP4CDS", 'dataSpec': dataSpec, 'expts': exptsSelected})
#    dataSpec = DataSpecification.objects.all()
#    return  render(request, 'qcapp/data-spec.html', {'page_title': "Data requested by CP4CDS", 'dataSpec': dataSpec} )


def variable_summary(request, variable):

    dataset = Dataset.objects.filter(variable=variable)
    return render(request, 'qcapp/variable-summary.html', {'dataset': dataset})

def var_qcplot(request, variable):

    dataset = Dataset.objects.filter(variable=variable)
    return render(request, 'qcapp/var-qcplot.html', {'page_title': 'Variable quality control plot','dataset': dataset})

def file_qc(request):


    return render(request, 'qcapp/file_qc.html', {'page_title': 'File:'})



def dataset_qc(request, variable):

    dataset = Dataset.objects.filter(variable=variable)
    return render(request, 'qcapp/var-qcplot.html', {'page_title': 'Variable quality control plot', 'dataset': dataset, })


def ag_test(request):
    dataSpec = DataSpecification.objects.all()
    return render(request, 'qcapp/ag-test.html', {'dataSpec': dataSpec,
                                                  'page_title': 'My great page!!!'})


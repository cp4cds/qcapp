from django.shortcuts import render
from django.http import HttpResponse

from qcapp.models import *
from .models import *

# Create your views here.
def happy(request, expt):
    ds = Dataset.objects.filter(experiment=expt).first()

    return HttpResponse("We found a Dataset with these attributes: %s, %s" % (ds.institute, ds.experiment))

def test(request):
    requestName = Request.objects.first().request_name
    return HttpResponse("I am a test." + requestName )

def data_spec(request):

    dataSpec = DataSpecification.objects.all()

    return render(request, 'qcapp/data-spec.html', {'dataSpec': dataSpec})

def variable_summary(request, variable):

    dataset = Dataset.objects.filter(variable=variable)
    return render(request, 'qcapp/variable-summary.html', {'dataset': dataset})

def var_qcplot(request, variable):

    dataset = Dataset.objects.filter(variable=variable)
    return render(request, 'qcapp/var-qcplot.html', {'dataset': dataset})



def ag_test(request):
    dataSpec = DataSpecification.objects.all()
    return render(request, 'qcapp/ag-test.html', {'dataSpec': dataSpec,
                                                  'page_title': 'My great page!!!'})


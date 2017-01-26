from django.shortcuts import render
from django.http import HttpResponse

from qcapp.models import *
from .models import DataRequest

# Create your views here.
def happy(request, expt):
    ds = Dataset.objects.filter(experiment=expt).first()

    return HttpResponse("We found a Dataset with these attributes: %s, %s" % (ds.institute, ds.experiment))

def test(request):
    requestName = Request.objects.first().request_name
    return HttpResponse("I am a test." + requestName )

def data_requests(request):

    dataRequests = DataRequest.objects.all()

    return render(request, 'qcapp/data-requests.html', {'dataRequests': dataRequests})

def ag_test(request):
    dataRequests = DataRequest.objects.all()
    return render(request, 'qcapp/ag-test.html', {'dataRequests': dataRequests,
                                                  'page_title': 'My great page!!!'})


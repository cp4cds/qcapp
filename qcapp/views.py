from django.shortcuts import render
from django.http import HttpResponse

from qcapp.models import *


# Create your views here.
def happy(request, expt):
    ds = Dataset.objects.filter(experiment=expt).first()

    return HttpResponse("We found a Dataset with these attributes: %s, %s" % (ds.institute, ds.experiment))

def test(request):
    return HttpResponse("I am a test.")

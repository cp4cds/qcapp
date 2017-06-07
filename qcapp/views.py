from django.shortcuts import render
from django.http import HttpResponse

from qcapp.models import *
from .models import *
from app_functions import *
import os


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

    dataSpec = DataSpecification.objects.filter(datarequesters__requested_by__contains='CP4CDS')
    return render(request, 'qcapp/data-spec-model.html', {'dataSpec': dataSpec})


def data_spec_experiment(request):

    dataSpec = DataSpecification.objects.all()

    return render(request, 'qcapp/data-spec-experiment.html', {'dataSpec': dataSpec})



def data_spec(request):


    exptsSelected = request.GET.keys()
    dataSpec = DataSpecification.objects.filter(datarequesters__requested_by__contains='CP4CDS')
    for spec in dataSpec:
        get_no_models_per_expt(spec, exptsSelected)

    return render(request, 'qcapp/data-spec.html', {'page_title': "Data requested", 'dataSpec': dataSpec, 'expts': exptsSelected})
#    dataSpec = DataSpecification.objects.all()
#    return  render(request, 'qcapp/data-spec.html', {'page_title': "Data requested by CP4CDS", 'dataSpec': dataSpec} )


def variable_summary(request, variable):

    dataset = Dataset.objects.filter(variable=variable)
    return render(request, 'qcapp/variable-summary.html', {'dataset': dataset})

def var_qcplot(request, variable):

    dataset = Dataset.objects.filter(variable=variable)
    return render(request, 'qcapp/var-qcplot.html', {'page_title': 'Variable quality control plot','dataset': dataset})

def file_qc(request):
    

    inst = set([])
    model = set([])
    expt = set([])
    freq = set([])
    realm = set([])
    table = set([])
    ens = set([])

    for i in Dataset.objects.all():
        inst.add(i.institute)
        model.add(i.institute)
        expt.add(i.institute)
        freq.add(i.institute)
        realm.add(i.institute)
        table.add(i.institute)
        ens.add(i.institute)

    file = DataFile.objects.first()
#    file = DataFile.objects.filter(institute=ins, model=model, experiment=expt, frequency=freq, realm=realm,
#                                   cmor_table=table, ensemble=ens)
    cf_qc = file.qccheck_set.filter(qc_check_type='CF')
    cedacc_qc = file.qccheck_set.filter(qc_check_type='CEDA-CC')

    ds_id = os.path.dirname(file.filepath).replace('/', '.')[1:]
    filen = os.path.basename(file.filepath)
    title = "Dataset: %s File: %s" % (ds_id, filen)


    return render(request, 'qcapp/file-qc.html',
                  {'page_title': title, 'cf_qc': cf_qc, 'cedacc_qc': cedacc_qc})
#                   'inst': inst, 'model': model, 'expt': freq, 'realm': realm, 'table': table, 'ens': ens}
#                  )

def dataset_qc(request, variable):

    dataset = Dataset.objects.filter(variable=variable)
    return render(request, 'qcapp/var-qcplot.html', {'page_title': 'Variable quality control plot', 'dataset': dataset, })


def ag_test(request):
    dataSpec = DataSpecification.objects.all()
    return render(request, 'qcapp/ag-test.html', {'dataSpec': dataSpec,
                                                  'page_title': 'My great page!!!'})


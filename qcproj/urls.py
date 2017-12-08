"""qcproj URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin

import qcapp.views

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^$', qcapp.views.home, name="home"),
    url(r'^documentation/', qcapp.views.documentation),
    url(r'^qcerrors/', qcapp.views.qcerrors),
    url(r'^ceda-cc-errors/', qcapp.views.ceda_cc_errors, name="ceda-cc-errors"),
    url(r'^cf-errors/', qcapp.views.cf_errors, name="cf-errors"),
    url(r'^variable-qc/(?P<ncfile>\S+)', qcapp.views.variable_qc, name="variable-qc"),
    url(r'^variable-file-qc/(?P<ncfile>\S+)', qcapp.views.variable_file_qc, name="variable-file-qc"),
    url(r'^variable-timeseries-qc/(?P<ncfile>\S+)', qcapp.views.variable_timeseries_qc, name="variable-timeseries-qc"),
    url(r'^variable-dataset-qc/(?P<variable>\S+)', qcapp.views.variable_dataset_qc, name="variable-dataset-qc"),
    url(r'^variable-summary-qc/', qcapp.views.variable_summary_qc, name="variable-summary-qc"),
    url(r'^data-spec/', qcapp.views.data_spec, name="data-spec"),
    url(r'^model-details/', qcapp.views.model_details, name="model-details"),
    url(r'^variable-details/', qcapp.views.variable_details, name="variable-details"),
    url(r'^data-availability/', qcapp.views.data_availability_matrix, name="data-availability-matrix"),
    url(r'^help/', qcapp.views.help, name="help"),


    # Ajax endpoints
    url(r'^facet-filter/(?P<model>\S+)/(?P<experiment>\S+)',
        qcapp.views.facet_filter, name="facet-filter"),
    url(r'^data-availability-variables', qcapp.views.data_availability_variables, name="data-availability-variables"),
    url(r'^get_variable_details/(?P<variable>\S+)/(?P<table>\S+)/(?P<freq>\S+)', qcapp.views.get_variable_details, name="get_varaible_details"),
    url(r'^all_variables/$', qcapp.views.all_variables, name="all_variables"),

]
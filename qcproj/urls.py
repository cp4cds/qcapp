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
    url(r'^documentation/', qcapp.views.documentation),
    url(r'^variable-qc/(?P<ncfile>\S+)', qcapp.views.variable_qc, name="variable-qc"),
    url(r'^variable-file-qc/(?P<ncfile>\S+)', qcapp.views.variable_file_qc, name="variable-file-qc"),
    url(r'^variable-timeseries-qc/(?P<ncfile>\S+)', qcapp.views.variable_timeseries_qc, name="variable-timeseries-qc"),
    url(r'^variable-dataset-qc/(?P<variable>\S+)', qcapp.views.variable_dataset_qc, name="variable-dataset-qc"),
    url(r'^variable-summary-qc/', qcapp.views.variable_summary_qc, name="variable-summary-qc"),
    url(r'^data-spec/', qcapp.views.data_spec, name="data-spec"),
    url(r'^facet-filter/(?P<model>\S+)/(?P<experiment>\S+)',
        qcapp.views.facet_filter, name="facet-filter"),
]
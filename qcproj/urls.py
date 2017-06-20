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
    url(r'^file-qc/(?P<version>\S+)&(?P<ncfile>\S+)', qcapp.views.file_qc, name="file-qc"),
    url(r'^variable-timeseries-qc/', qcapp.views.variable_timeseries_qc, name="variable-timeseries-qc"),
#    url(r'^variable-qc/', qcapp.views.variable_qc, name="variable-qc"),
    url(r'^data-spec/', qcapp.views.data_spec, name="data-spec"),
    url(r'^data-spec-model/', qcapp.views.data_spec_model, name="data-spec-model"),
    url(r'var-qcplot/(?P<variable>\S+)', qcapp.views.var_qcplot, name="var-qcplot"),
    url(r'^dataset-qc/(?P<variable>\S+)', qcapp.views.dataset_qc, name="dataset-qc"),
    url(r'^(?P<variable>\S+)/', qcapp.views.variable_summary, name="variable-summary"),
]
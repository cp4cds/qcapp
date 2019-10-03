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

from qcapp.views import *

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^$', HomeView.as_view(), name="home"),
    url(r'^model-details/', model_details, name="model-details"),
    url(r'^variable-details/', variable_details, name="variable-details"),
    url(r'^data-availability/', data_availability_matrix, name="data-availability-matrix"),
    url(r'^help/', HelpView.as_view(), name="help"),


    # Ajax endpoints
    url(r'^facet-filter/(?P<model>\S+)/(?P<experiment>\S+)',
        facet_filter, name="facet-filter"),
    url(r'^get_variable_details/(?P<variable>\S+)/(?P<table>\S+)/(?P<freq>\S+)', get_variable_details, name="get_varaible_details"),

]
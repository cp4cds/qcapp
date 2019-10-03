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
from django.urls import path
from django.contrib import admin

from qcapp.views import *

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', HomeView.as_view(), name="home"),
    path('model-details/', model_details, name="model-details"),
    path('variable-details/', variable_details, name="variable-details"),
    path('data-availability/', data_availability_matrix, name="data-availability-matrix"),
    path('help/', HelpView.as_view(), name="help"),

    # Ajax endpoints
    path('facet-filter/<str:model>/<str:experiment>', facet_filter, name="facet-filter"),
    path('get_variable_details/<str:variable>/<str:table>/<str:freq>', get_variable_details,
         name="get_varaible_details"),

]

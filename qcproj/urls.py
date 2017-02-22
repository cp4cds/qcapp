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
    url(r'^happy/(.+)/', qcapp.views.happy),
    url(r'^admin/', admin.site.urls),
    url(r'^test/', qcapp.views.test),
    url(r'^data-spec/(?P<variable>\S+)', qcapp.views.var_qcplot, name="var-qcplot"),
    url(r'^data-spec/', qcapp.views.data_spec, name="data-spec"),
    url(r'^(?P<variable>\S+)/', qcapp.views.variable_summary, name="variable-summary"),
    url(r'^ag-test/', qcapp.views.ag_test),
]

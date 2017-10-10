from django.contrib import admin
from .models import DataRequester
from .models import DataSpecification
from .models import Dataset
from .models import DataFile
from .models import QCerror


# Register your models here.
admin.site.register(DataRequester)
admin.site.register(DataSpecification)
admin.site.register(Dataset)
admin.site.register(DataFile)
admin.site.register(QCerror)

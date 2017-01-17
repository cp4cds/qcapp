import django
django.setup()

from qcapp.models import *
print "Counts..."
print "Dataset:", Dataset.objects.count()
print "DataFile:", DataFile.objects.count()

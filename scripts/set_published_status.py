
from setup_django import *
import os
import sys
import shutil

datasets_in_psql = '../ancil_files/c3s-cmip5_dataset_versions_in_psql.log'

with open(datasets_in_psql) as r:
	data = r.readlines()


for d in data[1:]:
	d = d.strip().replace('c3s-cmip5','CMIP5')
	ds = Dataset.objects.filter(dataset_id=d).first()
	ds.published = True
	ds.save()




# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-03-27 09:49
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0013_auto_20170320_1501'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='datasetqc',
            name='check_dataset',
        ),
        migrations.RemoveField(
            model_name='datasetqc',
            name='qc_check',
        ),
        migrations.RemoveField(
            model_name='fileqc',
            name='check_file',
        ),
        migrations.RemoveField(
            model_name='fileqc',
            name='qc_check',
        ),
        migrations.RemoveField(
            model_name='qccheck',
            name='file_qc',
        ),
        migrations.RemoveField(
            model_name='qcerror',
            name='qc_check',
        ),
        migrations.RemoveField(
            model_name='qcpercentiles',
            name='dataset',
        ),
        migrations.RemoveField(
            model_name='qcpercentiles',
            name='qc_plot',
        ),
        migrations.RemoveField(
            model_name='qcplot',
            name='dataset',
        ),
        migrations.RemoveField(
            model_name='qcplot',
            name='qc_percentiles',
        ),
        migrations.RemoveField(
            model_name='qcresults',
            name='dataset_qc',
        ),
        migrations.RemoveField(
            model_name='qcresults',
            name='file_qc',
        ),
        migrations.DeleteModel(
            name='DatasetQC',
        ),
        migrations.DeleteModel(
            name='FileQC',
        ),
        migrations.DeleteModel(
            name='QCcheck',
        ),
        migrations.DeleteModel(
            name='QCerror',
        ),
        migrations.DeleteModel(
            name='QCpercentiles',
        ),
        migrations.DeleteModel(
            name='QCplot',
        ),
        migrations.DeleteModel(
            name='QCresults',
        ),
    ]
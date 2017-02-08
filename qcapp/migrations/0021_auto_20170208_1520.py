# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-02-08 15:20
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0020_auto_20170208_1404'),
    ]

    operations = [
        migrations.RenameField(
            model_name='datafile',
            old_name='filepath',
            new_name='filename',
        ),
        migrations.AlterField(
            model_name='datafile',
            name='tracking_id',
            field=models.CharField(blank=True, max_length=80),
        ),
        migrations.AlterField(
            model_name='qcresults',
            name='dataset_qc',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='qcapp.DatasetQC'),
        ),
        migrations.AlterField(
            model_name='qcresults',
            name='file_qc',
            field=models.ManyToManyField(to='qcapp.FileQC'),
        ),
    ]

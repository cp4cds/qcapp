# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-02-08 14:04
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0019_auto_20170202_1548'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='exists',
            field=models.BooleanField(default=False),
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
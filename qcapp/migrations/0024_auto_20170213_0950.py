# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-02-13 09:50
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0023_auto_20170213_0922'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataspecification',
            name='dataset_qc',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='qcapp.DatasetQC'),
        ),
        migrations.AddField(
            model_name='dataspecification',
            name='file_qc',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='qcapp.FileQC'),
        ),
        migrations.AlterField(
            model_name='fileqc',
            name='ceda_cc_score',
            field=models.PositiveSmallIntegerField(default=0),
        ),
        migrations.AlterField(
            model_name='fileqc',
            name='cf_compliance_score',
            field=models.PositiveSmallIntegerField(default=0),
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

# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-03-20 15:01
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0012_qccheck_file_qc'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='qccheck',
            name='qc_check_file',
        ),
        migrations.RemoveField(
            model_name='fileqc',
            name='qc_check',
        ),
        migrations.AddField(
            model_name='fileqc',
            name='qc_check',
            field=models.ForeignKey(blank=True, default='', on_delete=django.db.models.deletion.CASCADE, to='qcapp.QCcheck'),
            preserve_default=False,
        ),
    ]
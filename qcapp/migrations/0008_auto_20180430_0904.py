# -*- coding: utf-8 -*-
# Generated by Django 1.11.7 on 2018-04-30 09:04
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0007_datafile_restricted'),
    ]

    operations = [
        migrations.AlterField(
            model_name='qcerror',
            name='error_msg',
            field=models.CharField(blank=True, max_length=800, null=True),
        ),
    ]

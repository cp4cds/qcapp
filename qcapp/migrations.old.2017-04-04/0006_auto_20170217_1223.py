# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-02-17 12:23
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0005_auto_20170217_1126'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataspecification',
            name='data_volume',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
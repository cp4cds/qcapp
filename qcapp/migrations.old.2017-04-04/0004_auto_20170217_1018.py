# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-02-17 10:18
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0003_dataspecification_data_volume'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datafile',
            name='size',
            field=models.DecimalField(blank=True, decimal_places=10, max_digits=30),
        ),
    ]
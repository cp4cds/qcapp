# -*- coding: utf-8 -*-
# Generated by Django 1.11.7 on 2018-03-28 10:27
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0002_auto_20171117_1433'),
    ]

    operations = [
        migrations.AddField(
            model_name='datafile',
            name='qc',
            field=models.NullBooleanField(default=False),
        ),
    ]
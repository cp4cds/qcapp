# -*- coding: utf-8 -*-
# Generated by Django 1.11.7 on 2018-06-12 08:57
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0020_qcerror_set'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='is_timeseries',
            field=models.NullBooleanField(default=None),
        ),
    ]
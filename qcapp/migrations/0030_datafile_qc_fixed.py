# -*- coding: utf-8 -*-
# Generated by Django 1.11.15 on 2018-12-07 16:07
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0029_auto_20181113_1219'),
    ]

    operations = [
        migrations.AddField(
            model_name='datafile',
            name='qc_fixed',
            field=models.NullBooleanField(default=False),
        ),
    ]

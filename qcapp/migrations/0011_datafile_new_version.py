# -*- coding: utf-8 -*-
# Generated by Django 1.11.7 on 2018-05-17 09:50
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0010_datafile_duplicate_of'),
    ]

    operations = [
        migrations.AddField(
            model_name='datafile',
            name='new_version',
            field=models.NullBooleanField(default=False),
        ),
    ]
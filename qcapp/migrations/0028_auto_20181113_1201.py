# -*- coding: utf-8 -*-
# Generated by Django 1.11.15 on 2018-11-13 12:01
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0027_auto_20181113_1158'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datafile',
            name='up_to_date_note',
            field=models.CharField(blank=True, default=None, max_length=2000, null=True),
        ),
    ]
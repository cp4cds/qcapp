# -*- coding: utf-8 -*-
# Generated by Django 1.11.7 on 2018-05-17 11:31
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0015_auto_20180517_1123'),
    ]

    operations = [
        migrations.AlterField(
            model_name='qcerror',
            name='error_level',
            field=models.CharField(blank=True, max_length=1000, null=True),
        ),
    ]
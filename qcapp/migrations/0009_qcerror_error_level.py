# -*- coding: utf-8 -*-
# Generated by Django 1.11.7 on 2018-04-30 13:49
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0008_auto_20180430_0904'),
    ]

    operations = [
        migrations.AddField(
            model_name='qcerror',
            name='error_level',
            field=models.CharField(blank=True, max_length=20, null=True),
        ),
    ]

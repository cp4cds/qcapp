# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-02-02 14:18
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0008_auto_20170202_1409'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='DataRequest',
            new_name='DataSpecification',
        ),
    ]

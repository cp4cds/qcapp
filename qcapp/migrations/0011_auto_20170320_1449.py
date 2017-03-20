# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-03-20 14:49
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('qcapp', '0010_cfresults'),
    ]

    operations = [
        migrations.CreateModel(
            name='QCerror',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('qc_error', models.CharField(max_length=300)),
            ],
        ),
        migrations.RemoveField(
            model_name='cfresults',
            name='qc_file',
        ),
        migrations.RemoveField(
            model_name='qccheck',
            name='qc_check_dataset',
        ),
        migrations.RemoveField(
            model_name='qccheck',
            name='qc_details',
        ),
        migrations.RemoveField(
            model_name='qccheck',
            name='qc_score',
        ),
        migrations.DeleteModel(
            name='CFResults',
        ),
        migrations.AddField(
            model_name='qcerror',
            name='qc_check',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='qcapp.QCcheck'),
        ),
    ]

# -*- coding: utf-8 -*-
# Generated by Django 1.9.7 on 2016-09-17 22:43
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('TestApp', '0004_auto_20160917_2230'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='d',
            name='d_for_b',
        ),
        migrations.AlterField(
            model_name='c',
            name='c_for',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='TestApp.B'),
        ),
        migrations.DeleteModel(
            name='D',
        ),
    ]

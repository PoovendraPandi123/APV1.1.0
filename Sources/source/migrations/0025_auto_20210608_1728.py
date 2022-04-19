# Generated by Django 3.2 on 2021-06-08 11:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('source', '0024_auto_20210608_1617'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='processinglayer',
            name='m_processing_layer',
        ),
        migrations.RemoveField(
            model_name='processinglayer',
            name='m_processing_sub_layer',
        ),
        migrations.RemoveField(
            model_name='processinglayerdefinition',
            name='m_aggregators',
        ),
        migrations.RemoveField(
            model_name='processinglayerdefinition',
            name='processing_layer',
        ),
        migrations.AddField(
            model_name='processinglayer',
            name='m_processing_layer_id',
            field=models.PositiveIntegerField(default=0, verbose_name='MasterProcessingLayer Id (Auto Generated)'),
        ),
        migrations.AddField(
            model_name='processinglayer',
            name='m_processing_sub_layer_id',
            field=models.PositiveIntegerField(default=0, verbose_name='MasterProcessingSubLayer Id (Auto Generated)'),
        ),
        migrations.AddField(
            model_name='processinglayerdefinition',
            name='m_aggregators_id',
            field=models.PositiveIntegerField(default=0, verbose_name='MasterAggregator Id (Auto Generated)'),
        ),
        migrations.AddField(
            model_name='processinglayerdefinition',
            name='processing_layer_id',
            field=models.PositiveIntegerField(default=0, verbose_name='ProcessingLayer Id (Auto Generated)'),
        ),
    ]

# Generated by Django 3.2 on 2021-06-13 17:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('source', '0026_alter_processinglayerdefinition_side'),
    ]

    operations = [
        migrations.AlterField(
            model_name='businesslayer',
            name='processing_layer_id',
            field=models.PositiveIntegerField(verbose_name='ProcessingLayer Id (Auto Generated)'),
        ),
    ]

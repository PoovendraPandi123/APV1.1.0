# Generated by Django 3.1.7 on 2021-12-15 00:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('source', '0003_auto_20211215_0255'),
    ]

    operations = [
        migrations.AlterField(
            model_name='mastersourcedefinitions',
            name='attribute_max_length',
            field=models.CharField(max_length=5, null=True, verbose_name='Attribute Maximum Length'),
        ),
        migrations.AlterField(
            model_name='mastersourcedefinitions',
            name='attribute_min_length',
            field=models.CharField(max_length=5, null=True, verbose_name='Attribute Minimum Length'),
        ),
    ]

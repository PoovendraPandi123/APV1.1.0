# Generated by Django 3.2 on 2021-07-01 13:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ModuleDetails', '0004_rename_main_model_userroles_sub_model'),
    ]

    operations = [
        migrations.AlterField(
            model_name='submodels',
            name='sub_model_name',
            field=models.CharField(max_length=64, verbose_name='Sub Module Name'),
        ),
    ]

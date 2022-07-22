# Generated by Django 3.1.7 on 2022-07-18 14:55

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('process', '0017_auto_20220715_0029'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='sourcerelations',
            name='is_related',
        ),
        migrations.AddField(
            model_name='sources',
            name='is_related',
            field=models.BooleanField(default=False, verbose_name='Related (Yes/No)'),
        ),
    ]

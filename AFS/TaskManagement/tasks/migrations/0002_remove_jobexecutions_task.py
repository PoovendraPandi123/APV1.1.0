# Generated by Django 3.1.7 on 2021-12-29 14:30

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('tasks', '0001_initial'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='jobexecutions',
            name='task',
        ),
    ]

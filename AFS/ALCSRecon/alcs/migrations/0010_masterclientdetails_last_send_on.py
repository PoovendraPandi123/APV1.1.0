# Generated by Django 3.1.7 on 2022-01-28 05:35

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('alcs', '0009_masterclientdetails_frequency'),
    ]

    operations = [
        migrations.AddField(
            model_name='masterclientdetails',
            name='last_send_on',
            field=models.CharField(max_length=64, null=True, verbose_name='Last Sent On'),
        ),
    ]

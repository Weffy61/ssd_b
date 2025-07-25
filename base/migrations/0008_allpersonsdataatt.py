# Generated by Django 5.2 on 2025-07-22 01:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('base', '0007_email_phone_person_emails_person_phones'),
    ]

    operations = [
        migrations.CreateModel(
            name='AllPersonsDataAtt',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('first_name', models.CharField(max_length=100, null=True)),
                ('last_name', models.CharField(max_length=100, null=True)),
                ('middle_name', models.CharField(blank=True, max_length=100, null=True)),
                ('ssn', models.CharField(blank=True, max_length=10, null=True)),
                ('dob', models.DateField(blank=True, null=True)),
                ('address', models.TextField(blank=True, null=True)),
                ('city', models.CharField(blank=True, max_length=100, null=True)),
                ('state', models.CharField(blank=True, max_length=100, null=True)),
                ('zip_code', models.CharField(blank=True, max_length=50, null=True)),
                ('phone_1', models.CharField(blank=True, max_length=15, null=True)),
                ('phone_2', models.CharField(blank=True, max_length=15, null=True)),
                ('email', models.CharField(max_length=250, unique=True)),
            ],
        ),
    ]

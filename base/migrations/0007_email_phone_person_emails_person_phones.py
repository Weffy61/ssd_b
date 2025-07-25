# Generated by Django 5.2 on 2025-07-20 19:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('base', '0006_personaddress_trgm_idx_address'),
    ]

    operations = [
        migrations.CreateModel(
            name='Email',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('email', models.CharField(max_length=250, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='Phone',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('phone_number', models.CharField(max_length=15, unique=True)),
            ],
        ),
        migrations.AddField(
            model_name='person',
            name='emails',
            field=models.ManyToManyField(related_name='persons', to='base.email'),
        ),
        migrations.AddField(
            model_name='person',
            name='phones',
            field=models.ManyToManyField(related_name='persons', to='base.phone'),
        ),
    ]

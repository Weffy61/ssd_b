from django.db import migrations, models


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ('base', '0002_alter_person_unique_together_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='person',
            name='first_name',
            field=models.CharField(max_length=100, null=True),
        ),
        migrations.AlterField(
            model_name='person',
            name='last_name',
            field=models.CharField(max_length=100, null=True),
        ),
        migrations.AlterField(
            model_name='person',
            name='middle_name',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AlterField(
            model_name='person',
            name='ssn',
            field=models.CharField(blank=True, max_length=10, null=True),
        ),
        migrations.AlterField(
            model_name='personaddress',
            name='address',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='personaddress',
            name='city',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AlterField(
            model_name='personaddress',
            name='county',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AlterField(
            model_name='personaddress',
            name='phone',
            field=models.CharField(blank=True, max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='personaddress',
            name='state',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AlterField(
            model_name='personaddress',
            name='zip_code',
            field=models.CharField(blank=True, max_length=50, null=True),
        ),

        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS base_person_first_name_idx ON base_person(first_name);",
            "DROP INDEX IF EXISTS base_person_first_name_idx;"
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS base_person_last_name_idx ON base_person(last_name);",
            "DROP INDEX IF EXISTS base_person_last_name_idx;"
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS base_person_middle_name_idx ON base_person(middle_name);",
            "DROP INDEX IF EXISTS base_person_middle_name_idx;"
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS base_person_ssn_idx ON base_person(ssn);",
            "DROP INDEX IF EXISTS base_person_ssn_idx;"
        ),

        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS base_personaddress_address_idx ON base_personaddress(address);",
            "DROP INDEX IF EXISTS base_personaddress_address_idx;"
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS base_personaddress_city_idx ON base_personaddress(city);",
            "DROP INDEX IF EXISTS base_personaddress_city_idx;"
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS base_personaddress_county_idx ON base_personaddress(county);",
            "DROP INDEX IF EXISTS base_personaddress_county_idx;"
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS base_personaddress_phone_idx ON base_personaddress(phone);",
            "DROP INDEX IF EXISTS base_personaddress_phone_idx;"
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS base_personaddress_state_idx ON base_personaddress(state);",
            "DROP INDEX IF EXISTS base_personaddress_state_idx;"
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS base_personaddress_zip_code_idx ON base_personaddress(zip_code);",
            "DROP INDEX IF EXISTS base_personaddress_zip_code_idx;"
        ),
    ]
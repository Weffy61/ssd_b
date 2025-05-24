import time

from diskcache import Cache
from django.core.management import BaseCommand
from base.models import AllPersonsData, Person, PersonalData, PersonAddress
from django.db import transaction, connection
from django.db.models import Q


class Command(BaseCommand):
    help = 'Optimized population from AllPersonsData'

    BATCH_SIZE = 10000
    M2M_BATCH_SIZE = 50000

    def handle(self, *args, **options):
        self.stdout.write("Starting ultra-optimized migration...")

        start = time.time()
        self.migrate_data()
        end = time.time()
        total = end - start
        print(f'finished in {total:.4f} second(s)')

    @transaction.atomic
    def migrate_data(self):
        # 1. Создаем индексы для ускорения поиска
        self.create_temp_indexes()

        # 2. Основные этапы миграции
        self.create_unique_persons()
        self.create_unique_addresses()
        self.fast_create_relations()

        # 3. Удаляем временные индексы
        self.drop_temp_indexes()

    def create_temp_indexes(self):
        """Создаем временные индексы для ускорения"""
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS tmp_person_composite_idx 
                ON base_person(first_name, last_name, middle_name, ssn)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS tmp_address_composite_idx 
                ON base_personaddress(address, city, state, zip_code, phone)
            """)

    def drop_temp_indexes(self):
        """Удаляем временные индексы"""
        with connection.cursor() as cursor:
            cursor.execute("DROP INDEX IF EXISTS tmp_person_composite_idx")
            cursor.execute("DROP INDEX IF EXISTS tmp_address_composite_idx")

    def create_unique_persons(self):
        self.stdout.write("Creating unique persons (raw SQL)...")
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO base_person (first_name, last_name, middle_name, ssn)
                SELECT DISTINCT
                    NULLIF(ap.first_name, ''),
                    NULLIF(ap.last_name, ''),
                    NULLIF(ap.middle_name, ''),
                    NULLIF(ap.ssn, '')
                FROM base_allpersonsdata ap
                ON CONFLICT DO NOTHING
            """)
            self.stdout.write(f"Unique persons inserted via raw SQL: {cursor.rowcount}")

    def create_unique_addresses(self):
        self.stdout.write("Creating unique addresses (raw SQL)...")

        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO base_personaddress (address, city, county, state, zip_code, phone)
                SELECT DISTINCT
                    NULLIF(SUBSTRING(address FROM 1 FOR 200), ''),
                    NULLIF(SUBSTRING(city FROM 1 FOR 100), ''),
                    NULLIF(SUBSTRING(county FROM 1 FOR 100), ''),
                    NULLIF(SUBSTRING(state FROM 1 FOR 100), ''),
                    NULLIF(SUBSTRING(zip_code FROM 1 FOR 50), ''),
                    NULLIF(SUBSTRING(phone FROM 1 FOR 50), '')
                FROM base_allpersonsdata
                WHERE address IS NOT NULL
                ON CONFLICT DO NOTHING
            """)
            self.stdout.write(f"Unique addresses inserted via raw SQL: {cursor.rowcount}")

    def fast_create_relations(self):
        """Супер-оптимизированное создание связей"""
        self.stdout.write("FAST creating relations...")

        self.stdout.write("Step 1: Creating PersonalData...")

        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO base_personaldata (
                    dob, name_suffix, alt1_dob, alt2_dob, alt3_dob,
                    aka1_fullname, aka2_fullname, aka3_fullname, person_id
                )
                SELECT 
                    ap.dob, ap.name_suffix, ap.alt1_dob, ap.alt2_dob, ap.alt3_dob,
                    ap.aka1_fullname, ap.aka2_fullname, ap.aka3_fullname, p.id
                FROM base_allpersonsdata ap
                JOIN base_person p ON 
                    p.first_name = ap.first_name AND
                    p.last_name = ap.last_name AND
                    (p.middle_name = ap.middle_name OR (p.middle_name IS NULL AND ap.middle_name IS NULL)) AND
                    (p.ssn = ap.ssn OR (p.ssn IS NULL AND ap.ssn IS NULL))
                WHERE 
                    ap.dob IS NOT NULL OR
                    (ap.name_suffix IS NOT NULL AND ap.name_suffix <> '') OR
                    ap.alt1_dob IS NOT NULL OR
                    ap.alt2_dob IS NOT NULL OR
                    ap.alt3_dob IS NOT NULL OR
                    (ap.aka1_fullname IS NOT NULL AND ap.aka1_fullname <> '') OR
                    (ap.aka2_fullname IS NOT NULL AND ap.aka2_fullname <> '') OR
                    (ap.aka3_fullname IS NOT NULL AND ap.aka3_fullname <> '')
                ON CONFLICT DO NOTHING
            """)
            self.stdout.write(f"PersonalData inserted via raw SQL: {cursor.rowcount}")

        self.stdout.write("Step 2: Creating M2M relations with raw SQL...")

        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO base_person_home_addresses (person_id, personaddress_id)
                SELECT p.id, pa.id
                FROM base_allpersonsdata ap
                JOIN base_person p ON 
                    p.first_name = ap.first_name AND
                    p.last_name = ap.last_name AND
                    (p.middle_name = ap.middle_name OR (p.middle_name IS NULL AND ap.middle_name IS NULL)) AND
                    (p.ssn = ap.ssn OR (p.ssn IS NULL AND ap.ssn IS NULL))
                JOIN base_personaddress pa ON
                    (pa.address = ap.address OR (pa.address IS NULL AND ap.address IS NULL)) AND
                    (pa.city = ap.city OR (pa.city IS NULL AND ap.city IS NULL)) AND
                    (pa.state = ap.state OR (pa.state IS NULL AND ap.state IS NULL)) AND
                    (pa.zip_code = ap.zip_code OR (pa.zip_code IS NULL AND ap.zip_code IS NULL)) AND
                    (pa.phone = ap.phone OR (pa.phone IS NULL AND ap.phone IS NULL))
                WHERE ap.address IS NOT NULL
                ON CONFLICT DO NOTHING
            """)
            self.stdout.write(f"M2M relations created: {cursor.rowcount}")

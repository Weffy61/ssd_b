import time

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
        """Оптимизированное создание персон"""
        self.stdout.write("Creating unique persons (optimized)...")

        # Используем bulk_create с ignore_conflicts
        persons = AllPersonsData.objects.values_list(
            'first_name', 'last_name', 'middle_name', 'ssn'
        ).distinct()

        batch = []
        for i, data in enumerate(persons, 1):
            batch.append(Person(
                first_name=data[0] if data[0] else None,
                last_name=data[1] if data[1] else None,
                middle_name=data[2] if data[2] else None,
                ssn=data[3] if data[3] else None
            ))

            if i % self.BATCH_SIZE == 0:
                Person.objects.bulk_create(batch, ignore_conflicts=True)
                batch = []
                self.stdout.write(f"Persons processed: {i}")

        if batch:
            Person.objects.bulk_create(batch, ignore_conflicts=True)

    def create_unique_addresses(self):
        """Оптимизированное создание адресов"""
        self.stdout.write("Creating unique addresses (optimized)...")

        addresses = AllPersonsData.objects.exclude(address__isnull=True).values_list(
            'address', 'city', 'county', 'state', 'zip_code', 'phone'
        ).distinct()

        batch = []
        for i, data in enumerate(addresses, 1):
            batch.append(PersonAddress(
                address=data[0][:200] if data[0] else None,
                city=data[1][:100] if data[1] else None,
                county=data[2][:100] if data[2] else None,
                state=data[3][:100] if data[3] else None,
                zip_code=data[4][:50] if data[4] else None,
                phone=data[5][:50] if data[5] else None
            ))

            if i % self.BATCH_SIZE == 0:
                PersonAddress.objects.bulk_create(batch, ignore_conflicts=True)
                batch = []
                self.stdout.write(f"Addresses processed: {i}")

        if batch:
            PersonAddress.objects.bulk_create(batch, ignore_conflicts=True)

    def fast_create_relations(self):
        """Супер-оптимизированное создание связей"""
        self.stdout.write("FAST creating relations...")

        # 1. Сначала создаем все PersonalData
        self.stdout.write("Step 1: Creating PersonalData in bulk...")

        personal_data_batch = []
        all_data = AllPersonsData.objects.only(
            'first_name', 'last_name', 'middle_name', 'ssn',
            'dob', 'name_suffix', 'alt1_dob', 'alt2_dob', 'alt3_dob',
            'aka1_fullname', 'aka2_fullname', 'aka3_fullname'
        ).iterator()

        person_cache = {}  # Минимальный кэш для ID персон

        for i, record in enumerate(all_data, 1):
            # Используем кэш для быстрого поиска ID персоны
            person_key = (record.first_name, record.last_name, record.middle_name, record.ssn)

            if person_key not in person_cache:
                try:
                    person = Person.objects.only('id').get(
                        first_name=record.first_name,
                        last_name=record.last_name,
                        middle_name=record.middle_name,
                        ssn=record.ssn
                    )
                    person_cache[person_key] = person.id
                except Person.DoesNotExist:
                    continue

            has_data = any([
                record.dob,
                record.name_suffix not in (None, ''),
                record.alt1_dob,
                record.alt2_dob,
                record.alt3_dob,
                record.aka1_fullname not in (None, ''),
                record.aka2_fullname not in (None, ''),
                record.aka3_fullname not in (None, ''),
            ])
            if not has_data:
                continue

            personal_data_batch.append(PersonalData(
                dob=record.dob,
                name_suffix=record.name_suffix,
                alt1_dob=record.alt1_dob,
                alt2_dob=record.alt2_dob,
                alt3_dob=record.alt3_dob,
                aka1_fullname=record.aka1_fullname,
                aka2_fullname=record.aka2_fullname,
                aka3_fullname=record.aka3_fullname,
                person_id=person_cache[person_key]
            ))

            if i % self.BATCH_SIZE == 0:
                PersonalData.objects.bulk_create(personal_data_batch)
                personal_data_batch = []
                self.stdout.write(f"PersonalData processed: {i}")

        if personal_data_batch:
            PersonalData.objects.bulk_create(personal_data_batch)

        # 2. Затем создаем все M2M связи одним запросом
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
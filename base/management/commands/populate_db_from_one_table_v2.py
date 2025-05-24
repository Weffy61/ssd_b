import time
import gc

from django.core.management import BaseCommand
from base.models import AllPersonsData, Person, PersonalData, PersonAddress
from django.db import transaction, connection
from django.db.models import Q, Min, Max


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
        self.create_temp_indexes()

        self.create_unique_persons()
        self.create_unique_addresses()
        self.fast_create_relations()
        self.create_m2m_person_addresses()

        self.drop_temp_indexes()

    def create_temp_indexes(self):
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
        with connection.cursor() as cursor:
            cursor.execute("DROP INDEX IF EXISTS tmp_person_composite_idx")
            cursor.execute("DROP INDEX IF EXISTS tmp_address_composite_idx")

    def create_unique_persons(self):
        self.stdout.write("Creating unique persons (optimized)...")

        persons = AllPersonsData.objects.values_list(
            'first_name', 'last_name', 'middle_name', 'ssn'
        ).distinct().iterator()

        batch = []
        count = 0
        for data in persons:
            batch.append(Person(
                first_name=data[0] if data[0] else None,
                last_name=data[1] if data[1] else None,
                middle_name=data[2] if data[2] else None,
                ssn=data[3] if data[3] else None
            ))
            count += 1

            if count % self.BATCH_SIZE == 0:
                Person.objects.bulk_create(batch, ignore_conflicts=True)
                batch.clear()
                gc.collect()
                self.stdout.write(f"Persons processed: {count}")

        if batch:
            Person.objects.bulk_create(batch, ignore_conflicts=True)
            gc.collect()

    def create_unique_addresses(self):
        self.stdout.write("Creating unique addresses (optimized)...")

        addresses = AllPersonsData.objects.exclude(address__isnull=True).values_list(
            'address', 'city', 'county', 'state', 'zip_code', 'phone'
        ).distinct().iterator()

        batch = []
        count = 0
        for data in addresses:
            batch.append(PersonAddress(
                address=data[0][:200] if data[0] else None,
                city=data[1][:100] if data[1] else None,
                county=data[2][:100] if data[2] else None,
                state=data[3][:100] if data[3] else None,
                zip_code=data[4][:50] if data[4] else None,
                phone=data[5][:50] if data[5] else None
            ))
            count += 1

            if count % self.BATCH_SIZE == 0:
                PersonAddress.objects.bulk_create(batch, ignore_conflicts=True)
                batch.clear()
                gc.collect()
                self.stdout.write(f"Addresses processed: {count}")

        if batch:
            PersonAddress.objects.bulk_create(batch, ignore_conflicts=True)
            gc.collect()

    def fast_create_relations(self):
        self.stdout.write("FAST creating relations in chunks...")

        min_id = AllPersonsData.objects.aggregate(min_id=Min('id'))['min_id']
        max_id = AllPersonsData.objects.aggregate(max_id=Max('id'))['max_id']

        CHUNK_SIZE = 10000

        personal_data_batch = []
        total_processed = 0

        for chunk_start in range(min_id, max_id + 1, CHUNK_SIZE):
            chunk_end = chunk_start + CHUNK_SIZE

            self.stdout.write(f"Processing chunk {chunk_start} - {chunk_end}...")

            chunk = list(AllPersonsData.objects.filter(id__gte=chunk_start, id__lt=chunk_end).iterator())

            keys = set((r.first_name, r.last_name, r.middle_name, r.ssn) for r in chunk)

            persons = Person.objects.filter(
                Q(
                    *[
                        Q(first_name=f, last_name=l, middle_name=m, ssn=s)
                        for f, l, m, s in keys
                    ],
                    _connector=Q.OR
                )
            ).only('id', 'first_name', 'last_name', 'middle_name', 'ssn')

            person_cache = {
                (p.first_name, p.last_name, p.middle_name, p.ssn): p.id
                for p in persons
            }

            for record in chunk:
                person_key = (record.first_name, record.last_name, record.middle_name, record.ssn)
                person_id = person_cache.get(person_key)
                if not person_id:
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
                    person_id=person_id
                ))

                if len(personal_data_batch) >= self.BATCH_SIZE:
                    PersonalData.objects.bulk_create(personal_data_batch)
                    total_processed += len(personal_data_batch)
                    personal_data_batch.clear()
                    gc.collect()
                    self.stdout.write(f"Inserted {total_processed} personal data records...")

            # На всякий случай после чанка вставим остаток
            if personal_data_batch:
                PersonalData.objects.bulk_create(personal_data_batch)
                total_processed += len(personal_data_batch)
                personal_data_batch.clear()
                gc.collect()
                self.stdout.write(f"Inserted {total_processed} personal data records...")

            del chunk
            gc.collect()

    def create_m2m_person_addresses(self):
        self.stdout.write("Linking Persons with Addresses (M2M)...")

        min_id = AllPersonsData.objects.aggregate(min_id=Min('id'))['min_id']
        max_id = AllPersonsData.objects.aggregate(max_id=Max('id'))['max_id']

        CHUNK_SIZE = 10000

        m2m_relations = []
        total_relations = 0

        for chunk_start in range(min_id, max_id + 1, CHUNK_SIZE):
            chunk_end = chunk_start + CHUNK_SIZE
            self.stdout.write(f"Processing M2M chunk {chunk_start}-{chunk_end}...")

            chunk = list(AllPersonsData.objects.filter(id__gte=chunk_start, id__lt=chunk_end).iterator())

            person_keys = {(r.first_name, r.last_name, r.middle_name, r.ssn) for r in chunk}
            address_keys = {(r.address, r.city, r.county, r.state, r.zip_code, r.phone) for r in chunk if r.address}

            persons = Person.objects.filter(
                Q(*[Q(first_name=f, last_name=l, middle_name=m, ssn=s) for f, l, m, s in person_keys], _connector=Q.OR)
            ).only('id', 'first_name', 'last_name', 'middle_name', 'ssn')

            addresses = PersonAddress.objects.filter(
                Q(*[Q(address=a, city=c, county=ct, state=s, zip_code=z, phone=p) for a, c, ct, s, z, p in
                    address_keys], _connector=Q.OR)
            ).only('id', 'address', 'city', 'county', 'state', 'zip_code', 'phone')

            person_cache = {(p.first_name, p.last_name, p.middle_name, p.ssn): p for p in persons}
            address_cache = {(a.address, a.city, a.county, a.state, a.zip_code, a.phone): a for a in addresses}

            for record in chunk:
                person_key = (record.first_name, record.last_name, record.middle_name, record.ssn)
                address_key = (record.address, record.city, record.county, record.state, record.zip_code, record.phone)

                person = person_cache.get(person_key)
                address = address_cache.get(address_key)

                if person and address:
                    m2m_relations.append((person.id, address.id))

                if len(m2m_relations) >= self.M2M_BATCH_SIZE:
                    with connection.cursor() as cursor:
                        insert_values = ",".join(f"({p_id}, {a_id})" for p_id, a_id in m2m_relations)
                        cursor.execute(f"""
                            INSERT INTO base_person_home_addresses (person_id, personaddress_id)
                            VALUES {insert_values}
                            ON CONFLICT DO NOTHING
                        """)
                    total_relations += len(m2m_relations)
                    m2m_relations.clear()
                    gc.collect()
                    self.stdout.write(f"Inserted {total_relations} M2M relations...")

            # Остаток вставляем
            if m2m_relations:
                with connection.cursor() as cursor:
                    insert_values = ",".join(f"({p_id}, {a_id})" for p_id, a_id in m2m_relations)
                    cursor.execute(f"""
                        INSERT INTO base_person_home_addresses (person_id, personaddress_id)
                        VALUES {insert_values}
                        ON CONFLICT DO NOTHING
                    """)
                total_relations += len(m2m_relations)
                m2m_relations.clear()
                gc.collect()
                self.stdout.write(f"Inserted {total_relations} M2M relations...")

            del chunk
            gc.collect()

import time

from django.core.management import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Packet migration from base_allpersonsdata to base_persons"

    def handle(self, *args, **options):
        self.stdout.write("Creating unique persons (raw SQL)...")
        start = time.time()
        self.set_unlogged()
        self.disable_triggers()
        self.create_temp_indexes()

        self.populate_persons()

        self.drop_temp_indexes()
        self.enable_triggers()
        self.set_logged()

        total = time.time() - start
        self.stdout.write(f"Finished inserting unique persons in {total:.2f} seconds.")

    def set_unlogged(self):
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person_home_addresses SET UNLOGGED;")
            cursor.execute("ALTER TABLE base_personaldata SET UNLOGGED;")
            cursor.execute("ALTER TABLE base_personaddress SET UNLOGGED;")
            cursor.execute("ALTER TABLE base_person SET UNLOGGED;")

    def set_logged(self):
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person SET LOGGED;")
            cursor.execute("ALTER TABLE base_personaddress SET LOGGED;")
            cursor.execute("ALTER TABLE base_personaldata SET LOGGED;")
            cursor.execute("ALTER TABLE base_person_home_addresses SET LOGGED;")

    def disable_triggers(self):
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person DISABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_personaddress DISABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_personaldata DISABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_person_home_addresses DISABLE TRIGGER USER;")

    def enable_triggers(self):
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person ENABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_personaddress ENABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_personaldata ENABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_person_home_addresses ENABLE TRIGGER USER;")

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

    def populate_persons(self):
        self.stdout.write("Creating unique persons (raw SQL)...")
        with connection.cursor() as cursor:
            cursor.execute("""
                        INSERT INTO base_person (first_name, last_name, middle_name, ssn)
                        SELECT
                            NULLIF(first_name, ''),
                            NULLIF(last_name, ''),
                            NULLIF(middle_name, ''),
                            NULLIF(ssn, '')
                        FROM (
                            SELECT
                                first_name,
                                last_name,
                                middle_name,
                                ssn,
                                ROW_NUMBER() OVER (
                                    PARTITION BY first_name, last_name, middle_name
                                    ORDER BY ssn
                                ) as rn
                            FROM base_allpersonsdata
                        ) ap
                        WHERE ap.rn = 1
                        ON CONFLICT (first_name, last_name, middle_name, ssn) DO NOTHING;
                    """)
            self.stdout.write(f"Unique persons inserted via raw SQL: {cursor.rowcount}")


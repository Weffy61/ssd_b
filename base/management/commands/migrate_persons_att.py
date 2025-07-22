import time

from django.core.management import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Packet migration from base_allpersonsdataatt to base_persons"

    def handle(self, *args, **options):
        self.stdout.write("Creating unique persons (raw SQL)...")
        start = time.time()
        self.create_temp_indexes()

        self.populate_persons()

        self.drop_temp_indexes()

        total = time.time() - start
        self.stdout.write(f"Finished inserting unique persons in {total:.2f} seconds.")

    def create_temp_indexes(self):
        self.stdout.write("Create temp index...")
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS tmp_idx_att_person
                ON base_allpersonsdataatt (first_name, last_name, middle_name, ssn);
            """)

    def drop_temp_indexes(self):
        self.stdout.write("Drop temp index...")
        with connection.cursor() as cursor:
            cursor.execute("""
                DROP INDEX IF EXISTS tmp_idx_att_person;
            """)

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
                            FROM base_allpersonsdataatt
                        ) ap
                        WHERE ap.rn = 1
                        ON CONFLICT (first_name, last_name, middle_name, ssn) DO NOTHING;
                    """)
            self.stdout.write(f"Unique persons inserted via raw SQL: {cursor.rowcount}")
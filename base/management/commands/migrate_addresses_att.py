import time

from django.core.management import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Packet migration from base_allpersonsdataatt to base_personaddress"

    def handle(self, *args, **options):
        self.stdout.write("Creating unique addresses (raw SQL)...")
        start = time.time()
        self.create_temp_indexes()

        self.populate_addresses()

        self.drop_temp_indexes()

        self.analyze_table()

        total = time.time() - start
        self.stdout.write(f"Finished inserting unique addresses in {total:.2f} seconds.")

    def analyze_table(self):
        with connection.cursor() as cursor:
            self.stdout.write(f"Analyzing the table base_personaddress")
            cursor.execute("ANALYZE base_personaddress;")

    def create_temp_indexes(self):
        self.stdout.write("🔧 Create indexes on base_allpersonsdataatt ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS tmp_idx_att_address
                ON base_allpersonsdataatt (address, city, state, zip_code);
            """)

    def drop_temp_indexes(self):
        self.stdout.write("🔧 Remove indexes on base_allpersonsdataatt ...")
        with connection.cursor() as cursor:
            cursor.execute("DROP INDEX IF EXISTS tmp_idx_att_address")

    def populate_addresses(self):
        self.stdout.write("🚀 Starting one-pass data migration...")

        self.stdout.write("📤 Inserting into base_personaddress...")
        with connection.cursor() as cursor:
            cursor.execute("""
                            INSERT INTO base_personaddress (address, city, county, state, zip_code, phone)
                            SELECT
                                NULLIF(SUBSTRING(address FROM 1 FOR 200), ''),
                                NULLIF(SUBSTRING(city FROM 1 FOR 100), ''),
                                NULL,
                                NULLIF(SUBSTRING(state FROM 1 FOR 100), ''),
                                NULLIF(SUBSTRING(zip_code FROM 1 FOR 50), ''),
                                NULL
                            FROM (
                                SELECT
                                    address,
                                    city,
                                    state,
                                    zip_code,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY address, city, state, zip_code
                                        ORDER BY zip_code
                                    ) as rn
                                FROM base_allpersonsdataatt
                            ) ap
                            WHERE ap.rn = 1
                            ON CONFLICT (address, city, county, state, zip_code, phone) DO NOTHING;
                        """)
            self.stdout.write(f"Unique addresses inserted via raw SQL: {cursor.rowcount}")


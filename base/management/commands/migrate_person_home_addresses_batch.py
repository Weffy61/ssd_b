import time
from django.core.management.base import BaseCommand
from django.db import connection, transaction


BATCH_SIZE = 300_000_000


class Command(BaseCommand):
    help = "Batch M2M migration into base_person_home_addresses"

    def set_unlogged(self):
        self.stdout.write("🔧 Set Unlogged on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person_home_addresses SET UNLOGGED;")

    def set_logged(self):
        self.stdout.write("🔧 Set Logged on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person_home_addresses SET LOGGED;")

    def disable_triggers(self):
        self.stdout.write("🔧 Disable Triggers on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person_home_addresses DISABLE TRIGGER USER;")

    def enable_triggers(self):
        self.stdout.write("🔧 Enable Triggers on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person_home_addresses ENABLE TRIGGER USER;")

    def remove_fk(self):
        self.stdout.write("🔧 Removing FK on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE base_person_home_addresses 
                DROP CONSTRAINT IF EXISTS base_person_home_addresses_person_id_9eef68da_fk_base_person_id;
                ALTER TABLE base_person_home_addresses
                DROP CONSTRAINT IF EXISTS base_person_home_add_personaddress_id_5ebf5ef5_fk_base_pers;
            """)

    def remove_index(self):
        self.stdout.write("🔧 Removing constraints and indexes on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE base_person_home_addresses 
                DROP CONSTRAINT IF EXISTS base_person_home_address_person_id_personaddress__09800e2a_uniq;
            """)
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute("""
                DROP INDEX CONCURRENTLY IF EXISTS base_person_home_addresses_person_id_9eef68da;
            """)
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute("""
                DROP INDEX CONCURRENTLY IF EXISTS base_person_home_addresses_personaddress_id_5ebf5ef5;
            """)
        connection.commit()

    def disable_autovacuum(self):
        self.stdout.write("🔧 Disable autovacuum on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("""
            ALTER TABLE base_person_home_addresses SET (autovacuum_enabled = false);
            """)

    def restore_index(self):
        self.stdout.write("🔧 Restoring indexes on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE base_person_home_addresses
                ADD CONSTRAINT base_person_home_address_person_id_personaddress__09800e2a_uniq
                UNIQUE (person_id, personaddress_id);
            """)
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE INDEX CONCURRENTLY base_person_home_addresses_person_id_9eef68da
                ON base_person_home_addresses (person_id);
            """)
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE INDEX CONCURRENTLY base_person_home_addresses_personaddress_id_5ebf5ef5
                ON base_person_home_addresses (personaddress_id);
            """)
        connection.commit()

    def restore_fk(self):
        self.stdout.write("🔧 Restoring FK on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                            ALTER TABLE base_person_home_addresses
                                ADD CONSTRAINT base_person_home_addresses_person_id_9eef68da_fk_base_person_id
                                FOREIGN KEY (person_id) REFERENCES base_person(id) NOT VALID;
                            ALTER TABLE base_person_home_addresses
                                ADD CONSTRAINT base_person_home_add_personaddress_id_5ebf5ef5_fk_base_pers
                                FOREIGN KEY (personaddress_id) REFERENCES base_personaddress(id) NOT VALID;
                        """)

    def restore_autovacuum(self):
        self.stdout.write("🔧 Enable Autovacuum on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE base_person_home_addresses SET (autovacuum_enabled = true);
            """)

    def validate_fk(self):
        self.stdout.write("🔧 Validating FK on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                        ALTER TABLE base_person_home_addresses VALIDATE CONSTRAINT base_person_home_addresses_person_id_9eef68da_fk_base_person_id;
                        ALTER TABLE base_person_home_addresses VALIDATE CONSTRAINT base_person_home_add_personaddress_id_5ebf5ef5_fk_base_pers;
                    """)

    def delete_dup(self):
        self.stdout.write("🔧 Remove duplicates on base_person_home_addresses ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                        DELETE FROM base_person_home_addresses a
                        USING base_person_home_addresses b
                        WHERE a.ctid < b.ctid
                            AND a.person_id = b.person_id
                            AND a.personaddress_id = b.personaddress_id;
                    """)

    def vacuum_analyze(self):
        self.stdout.write("🧹 Running VACUUM ANALYZE on base_person_home_addresses (outside transaction) ...")
        conn = connection.connection
        conn.commit()
        with conn.cursor() as cursor:
            cursor.execute("VACUUM ANALYZE base_person_home_addresses;")

    def restore_settings(self):
        self.delete_dup()
        self.restore_index()
        self.restore_fk()
        self.restore_autovacuum()
        self.enable_triggers()
        self.set_logged()
        self.validate_fk()
        self.vacuum_analyze()

    def prepare_for_migrate(self):
        self.disable_triggers()
        self.set_unlogged()
        self.remove_fk()
        self.remove_index()
        self.disable_autovacuum()

    def handle(self, *args, **options):
        start_time = time.time()
        self.prepare_for_migrate()

        with connection.cursor() as cursor:
            cursor.execute("SELECT MIN(id), MAX(id) FROM base_allpersonsdata WHERE address IS NOT NULL")
            min_id, max_id = cursor.fetchone()

        if min_id is None:
            self.stdout.write("❗ No data to process.")
            return

        self.stdout.write(f"📊 Processing from ID {min_id} to {max_id} in chunks of {BATCH_SIZE}")

        for start_id in range(min_id, max_id + 1, BATCH_SIZE):
            end_id = min(start_id + BATCH_SIZE - 1, max_id)
            self.stdout.write(f"🚀 Inserting rows {start_id} to {end_id}")

            batch_start = time.time()

            insert_query = f"""
                SET LOCAL work_mem = '128MB';
                INSERT INTO base_person_home_addresses (person_id, personaddress_id)
                SELECT 
                    p.id, pa.id
                FROM base_allpersonsdata ap
                JOIN base_person p ON 
                    p.first_name = ap.first_name AND
                    p.last_name = ap.last_name AND
                    COALESCE(p.middle_name, '') = COALESCE(ap.middle_name, '') AND
                    COALESCE(p.ssn, '') = COALESCE(ap.ssn, '')
                JOIN base_personaddress pa ON 
                    COALESCE(pa.address, '') = COALESCE(ap.address, '') AND
                    COALESCE(pa.city, '') = COALESCE(ap.city, '') AND
                    COALESCE(pa.state, '') = COALESCE(ap.state, '') AND
                    COALESCE(pa.zip_code, '') = COALESCE(ap.zip_code, '') AND
                    COALESCE(pa.phone, '') = COALESCE(ap.phone, '')
                WHERE ap.id BETWEEN {start_id} AND {end_id};
            """

            with connection.cursor() as cursor:
                cursor.execute("SET synchronous_commit TO OFF;")
                cursor.execute("SET enable_nestloop TO OFF;")
                try:
                    with transaction.atomic():
                        cursor.execute(insert_query)
                except Exception as ex:
                    self.stdout.write(f"❌ Error in batch {start_id}-{end_id}: {ex}")
                    continue

            batch_end = time.time()
            self.stdout.write(f"✅ Batch {start_id}-{end_id} done in {batch_end - batch_start:.2f} seconds.")
        self.restore_settings()
        total_time = time.time() - start_time
        self.stdout.write(f"🎉 Full M2M migration finished in {total_time:.2f} seconds.")

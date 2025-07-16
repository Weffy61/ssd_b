import time
from django.core.management.base import BaseCommand
from django.db import connection, transaction


class Command(BaseCommand):
    help = "Full M2M migration into base_person_home_addresses_new"

    def set_unlogged(self):
        self.stdout.write("🔧 Set Unlogged on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person_home_addresses_new SET UNLOGGED;")

    def set_logged(self):
        self.stdout.write("🔧 Set Logged on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person_home_addresses_new SET LOGGED;")

    def disable_triggers(self):
        self.stdout.write("🔧 Disable Triggers on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person_home_addresses_new DISABLE TRIGGER USER;")

    def enable_triggers(self):
        self.stdout.write("🔧 Enable Triggers on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_person_home_addresses_new ENABLE TRIGGER USER;")

    def remove_fk(self):
        self.stdout.write("🔧 Removing FK on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE base_person_home_addresses_new 
                DROP CONSTRAINT IF EXISTS base_person_home_addresses_person_id_9eef68da_fk_base_person_id;
                ALTER TABLE base_person_home_addresses_new
                DROP CONSTRAINT IF EXISTS base_person_home_add_personaddress_id_5ebf5ef5_fk_base_pers;
            """)

    def disable_autovacuum(self):
        self.stdout.write("🔧 Disable autovacuum on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("""
            ALTER TABLE base_person_home_addresses_new SET (autovacuum_enabled = false);
            """)

    def restore_index(self):
        self.stdout.write("🔧 Restoring indexes on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE base_person_home_addresses_new
                ADD CONSTRAINT base_person_home_address_person_id_personaddress__09800e2a_uniq
                UNIQUE (person_id, personaddress_id);
            """)
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE INDEX CONCURRENTLY base_person_home_addresses_person_id_9eef68da
                ON base_person_home_addresses_new (person_id);
            """)
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE INDEX CONCURRENTLY base_person_home_addresses_personaddress_id_5ebf5ef5
                ON base_person_home_addresses_new (personaddress_id);
            """)
        connection.commit()

    def remove_index(self):
        self.stdout.write("🔧 Removing constraints and indexes on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE base_person_home_addresses_new 
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

    def restore_fk(self):
        self.stdout.write("🔧 Restoring FK on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                            ALTER TABLE base_person_home_addresses_new
                                ADD CONSTRAINT base_person_home_addresses_person_id_9eef68da_fk_base_person_id
                                FOREIGN KEY (person_id) REFERENCES base_person(id) NOT VALID;
                            ALTER TABLE base_person_home_addresses_new
                                ADD CONSTRAINT base_person_home_add_personaddress_id_5ebf5ef5_fk_base_pers
                                FOREIGN KEY (personaddress_id) REFERENCES base_personaddress(id) NOT VALID;
                        """)

    def restore_autovacuum(self):
        self.stdout.write("🔧 Enable Autovacuum on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE base_person_home_addresses_new SET (autovacuum_enabled = true);
            """)

    def validate_fk(self):
        self.stdout.write("🔧 Validating FK on base_person_home_addresses_new ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                        ALTER TABLE base_person_home_addresses_new VALIDATE CONSTRAINT base_person_home_addresses_person_id_9eef68da_fk_base_person_id;
                        ALTER TABLE base_person_home_addresses_new VALIDATE CONSTRAINT base_person_home_add_personaddress_id_5ebf5ef5_fk_base_pers;
                    """)

    def restore_settings(self):
        self.restore_index()
        self.restore_fk()
        self.restore_autovacuum()
        self.enable_triggers()
        self.set_logged()
        self.validate_fk()

    def prepare_for_migrate(self):
        self.disable_triggers()
        self.set_unlogged()
        self.remove_fk()
        self.remove_index()
        self.disable_autovacuum()

    def handle(self, *args, **options):
        start_time = time.time()
        self.prepare_for_migrate()

        self.stdout.write(f"📊 Processing migration ...")

        insert_query = f"""
                        SET LOCAL work_mem = '128MB';
                        INSERT INTO base_person_home_addresses_new (person_id, personaddress_id)
                        SELECT DISTINCT
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
                    """

        with connection.cursor() as cursor:
            cursor.execute("SET synchronous_commit TO OFF;")
            cursor.execute("SET enable_nestloop TO OFF;")
            try:
                with transaction.atomic():
                    cursor.execute(insert_query)
            except Exception as ex:
                self.stdout.write(f"❌ Error: {ex}")

        self.restore_settings()
        total_time = time.time() - start_time
        self.stdout.write(f"🎉 Full M2M migration finished in {total_time:.2f} seconds.")
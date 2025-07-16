import time
from django.core.management.base import BaseCommand
from django.db import connection, transaction


class Command(BaseCommand):
    help = "Prepare table for work after bulk insert"

    def restore_index(self):
        self.stdout.write("🔧 Restoring Index on base_personaldata.person_id ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                        CREATE INDEX CONCURRENTLY base_personaldata_person_id_0a11971a
                            ON base_personaldata(person_id);
                    """)

    def restore_fk(self):
        self.stdout.write("🔧 Restoring FK on base_personaldata.person_id ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                            ALTER TABLE base_personaldata
                                ADD CONSTRAINT base_personaldata_person_id_0a11971a_fk_base_person_id
                                FOREIGN KEY (person_id) REFERENCES base_person(id) NOT VALID;
                        """)

    def restore_autovacuum(self):
        self.stdout.write("🔧 Enable Autovacuum on base_personaldata ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE base_personaldata SET (autovacuum_enabled = true);
            """)

    def restore_triggers_logged(self):
        self.stdout.write("🔧 Set Logged and Triggers on base_personaldata ...")
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_personaldata ENABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_personaldata SET LOGGED;")

    def vacuum_analyze(self):
        self.stdout.write("🧹 Running VACUUM ANALYZE on base_personaldata (outside transaction) ...")
        conn = connection.connection
        conn.commit()
        with conn.cursor() as cursor:
            cursor.execute("VACUUM ANALYZE base_personaldata;")

    def handle(self, *args, **options):
        start_time = time.time()

        self.restore_index()
        self.restore_fk()
        self.restore_autovacuum()
        self.restore_triggers_logged()
        self.vacuum_analyze()

        total_time = time.time() - start_time
        self.stdout.write(f"🎉 Full setup finished in {total_time:.2f} seconds.")

import time
from django.core.management.base import BaseCommand
from django.db import connection, transaction


BATCH_SIZE = 300_000_000


class Command(BaseCommand):
    help = "Batch migration from base_allpersonsdata to base_personaldata (~100M per batch)"

    def remove_index_fk(self):
        self.stdout.write("🔧 Removing FK and index on base_personaldata.person_id ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE base_personaldata DROP CONSTRAINT IF EXISTS base_personaldata_person_id_0a11971a_fk_base_person_id;
                DROP INDEX IF EXISTS base_personaldata_person_id_0a11971a;
                ALTER TABLE base_personaldata SET (autovacuum_enabled = false);
            """)

    def restore_index_fk(self):
        self.stdout.write("🔧 Restoring FK and index on base_personaldata.person_id ...")
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE INDEX CONCURRENTLY base_personaldata_person_id_0a11971a
                    ON base_personaldata(person_id);
            """)
            cursor.execute("""
                ALTER TABLE base_personaldata
                    ADD CONSTRAINT base_personaldata_person_id_0a11971a_fk_base_person_id
                    FOREIGN KEY (person_id) REFERENCES base_person(id) NOT VALID;
            """)
            cursor.execute("""
                ALTER TABLE base_personaldata SET (autovacuum_enabled = true);
                VACUUM ANALYZE base_personaldata;
            """)

    def handle(self, *args, **options):
        start_time = time.time()
        self.remove_index_fk()

        with connection.cursor() as cursor:
            cursor.execute("SELECT MIN(id), MAX(id) FROM base_allpersonsdata")
            min_id, max_id = cursor.fetchone()

        if min_id is None:
            self.stdout.write("❗ No data to process.")
            return

        self.stdout.write(f"📊 Processing from ID {min_id} to {max_id} in chunks of {BATCH_SIZE}")

        resume_id = min_id + 700_000_000
        for start_id in range(resume_id, max_id + 1, BATCH_SIZE):
            end_id = min(start_id + BATCH_SIZE - 1, max_id)
            self.stdout.write(f"🚀 Inserting rows {start_id} to {end_id}")

            batch_start = time.time()

            insert_query = f"""
                SET LOCAL work_mem = '128MB';
                INSERT INTO base_personaldata (
                    dob, name_suffix, alt1_dob, alt2_dob, alt3_dob,
                    aka1_fullname, aka2_fullname, aka3_fullname, person_id
                )
                SELECT
                    ap.dob, ap.name_suffix, ap.alt1_dob, ap.alt2_dob, ap.alt3_dob,
                    ap.aka1_fullname, ap.aka2_fullname, ap.aka3_fullname, p.id
                FROM base_allpersonsdata ap
                JOIN base_person p ON (
                    p.first_name = ap.first_name AND
                    p.last_name = ap.last_name AND
                    COALESCE(p.middle_name, '') = COALESCE(ap.middle_name, '') AND
                    COALESCE(p.ssn, '') = COALESCE(ap.ssn, '')
                )
                WHERE
                    ap.id BETWEEN {start_id} AND {end_id} AND (
                        ap.dob IS NOT NULL OR
                        (ap.name_suffix IS NOT NULL AND ap.name_suffix <> '') OR
                        ap.alt1_dob IS NOT NULL OR
                        ap.alt2_dob IS NOT NULL OR
                        ap.alt3_dob IS NOT NULL OR
                        (ap.aka1_fullname IS NOT NULL AND ap.aka1_fullname <> '') OR
                        (ap.aka2_fullname IS NOT NULL AND ap.aka2_fullname <> '') OR
                        (ap.aka3_fullname IS NOT NULL AND ap.aka3_fullname <> '')
                    );
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

        self.restore_index_fk()
        total_time = time.time() - start_time
        self.stdout.write(f"🎉 Full migration finished in {total_time:.2f} seconds.")
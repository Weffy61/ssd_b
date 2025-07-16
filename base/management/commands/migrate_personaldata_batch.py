import time

from django.core.management.base import BaseCommand
from django.db import connection, transaction

BATCH_SIZE = 100_000


class Command(BaseCommand):
    help = "Packet migration from base_allpersonsdata to base_personaldata"

    def remove_index_fk(self):
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE base_personaldata DROP CONSTRAINT IF EXISTS base_personaldata_person_id_0a11971a_fk_base_person_id;")
            cursor.execute("DROP INDEX IF EXISTS base_personaldata_person_id_0a11971a;")
            cursor.execute("ALTER TABLE base_personaldata SET (autovacuum_enabled = false);")


    def add_index_fk(self):
        with connection.cursor as cursor:
            cursor.execute("CREATE INDEX CONCURRENTLY base_personaldata_person_id_0a11971a ON base_personaldata(person_id);")
            cursor.execute("ALTER TABLE base_personaldata VALIDATE CONSTRAINT base_personaldata_person_id_0a11971a_fk_base_person_id;")
            cursor.execute("ALTER TABLE base_personaldata SET (autovacuum_enabled = true);")
            cursor.execute("VACUUM ANALYZE base_personaldata;")

    def handle(self, *args, **options):
        start = time.time()

        with connection.cursor() as cursor:
            cursor.execute("SELECT MIN(id), MAX(id) FROM base_allpersonsdata")
            min_id, max_id = cursor.fetchone()
            cursor.execute("SET synchronous_commit TO OFF;")
        self.remove_index_fk()
        if min_id is None:
            self.stdout.write("❗ No data for migration.")
            return

        self.stdout.write(f"📌 ID range: {min_id} - {max_id}")
        iterations = 0
        for start_id in range(min_id, max_id + 1, BATCH_SIZE):
            if  iterations % 3 == 0:
                with connection.cursor() as cursor:
                    time.sleep(3)
                    cursor.execute("CHECKPOINT;")
                    cursor.execute("VACUUM ANALYZE base_personaldata;")
            end_id = start_id + BATCH_SIZE - 1
            self.stdout.write(f"🚀 Processing block: {start_id} - {end_id}")
            t0 = time.time()

            insert_query = f"""
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

            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL work_mem = '128MB';")
                    cursor.execute(insert_query)

            t1 = time.time()
            self.stdout.write(f"✅ Inserted block {start_id}-{end_id} in {t1 - t0:.2f}s")
            iterations += 1
        self.add_index_fk()
        total = time.time() - start
        self.stdout.write(f"🎉 Finished in {total:.2f} second(s)")
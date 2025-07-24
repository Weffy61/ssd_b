import time
from django.core.management.base import BaseCommand
from django.db import connection, transaction


BATCH_SIZE = 100_000_000


class Command(BaseCommand):
    help = "Batch migration from base_allpersonsdataatt to base_personaldata (~100M per batch)"

    def handle(self, *args, **options):
        start_time = time.time()

        with connection.cursor() as cursor:
            cursor.execute("SELECT MIN(id), MAX(id) FROM base_allpersonsdataatt")
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
                INSERT INTO base_personaldata (
                    dob, person_id
                )
                SELECT
                    ap.dob, p.id
                FROM base_allpersonsdataatt ap
                JOIN base_person p ON (
                    p.first_name = ap.first_name AND
                    p.last_name = ap.last_name AND
                    COALESCE(p.middle_name, '') = COALESCE(ap.middle_name, '') AND
                    COALESCE(p.ssn, '') = COALESCE(ap.ssn, '')
                )
                WHERE
                    ap.id BETWEEN {start_id} AND {end_id} AND ap.dob IS NOT NULL;
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

        total_time = time.time() - start_time
        self.stdout.write(f"🎉 Full migration finished in {total_time:.2f} seconds.")
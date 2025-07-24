import time
from django.core.management.base import BaseCommand
from django.db import connection, transaction


class Command(BaseCommand):
    help = "Full M2M migration into base_person_home_addresses from base_allpersonsdataatt"

    def handle(self, *args, **options):
        start_time = time.time()

        self.stdout.write(f"📊 Processing migration ...")

        insert_query = f"""
                        SET LOCAL work_mem = '128MB';
                        INSERT INTO base_person_home_addresses (person_id, personaddress_id)
                        SELECT DISTINCT
                            p.id, pa.id
                        FROM base_allpersonsdataatt ap
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
                            pa.county IS NULL AND
                            pa.phone is NULL
                        ON CONFLICT DO NOTHING;
                    """

        with connection.cursor() as cursor:
            cursor.execute("SET synchronous_commit TO OFF;")
            cursor.execute("SET enable_nestloop TO OFF;")
            try:
                with transaction.atomic():
                    cursor.execute(insert_query)
            except Exception as ex:
                self.stdout.write(f"❌ Error: {ex}")

        total_time = time.time() - start_time
        self.stdout.write(f"🎉 Full M2M migration finished in {total_time:.2f} seconds.")
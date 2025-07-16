import time
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "One-pass migration from base_allpersonsdata to base_personaldata"

    def handle(self, *args, **options):
        start_time = time.time()
        with connection.cursor() as cursor:
            self.stdout.write("🚀 Starting one-pass data migration...")

            self.stdout.write("📤 Inserting into base_personaldata...")
            cursor.execute("""
                INSERT INTO base_personaldata (
                    dob, name_suffix, alt1_dob, alt2_dob, alt3_dob,
                    aka1_fullname, aka2_fullname, aka3_fullname, person_id
                )
                SELECT
                    ap.dob, ap.name_suffix, ap.alt1_dob, ap.alt2_dob, ap.alt3_dob,
                    ap.aka1_fullname, ap.aka2_fullname, ap.aka3_fullname,
                    p.id
                FROM base_allpersonsdata ap
                JOIN base_person p ON
                    p.first_name = ap.first_name AND
                    p.last_name = ap.last_name AND
                    COALESCE(p.middle_name, '') = COALESCE(ap.middle_name, '') AND
                    COALESCE(p.ssn, '') = COALESCE(ap.ssn, '')
                WHERE
                    ap.dob IS NOT NULL OR
                    (ap.name_suffix IS NOT NULL AND ap.name_suffix <> '') OR
                    ap.alt1_dob IS NOT NULL OR
                    ap.alt2_dob IS NOT NULL OR
                    ap.alt3_dob IS NOT NULL OR
                    (ap.aka1_fullname IS NOT NULL AND ap.aka1_fullname <> '') OR
                    (ap.aka2_fullname IS NOT NULL AND ap.aka2_fullname <> '') OR
                    (ap.aka3_fullname IS NOT NULL AND ap.aka3_fullname <> '');
            """)
            self.stdout.write("✅ Insert complete")

        end_time = time.time()
        total = end_time - start_time
        self.stdout.write(f"🎉 Finished in {total:.2f} seconds.")

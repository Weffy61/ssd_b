import time
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Add array expression index for person match"
    requires_atomic = False

    def handle(self, *args, **options):
        start_time = time.time()
        with connection.cursor() as cursor:
            self.stdout.write("🚀 Creating array-based index...")

            cursor.execute("""
                CREATE INDEX CONCURRENTLY person_name_ssn_idx
                ON base_person (
                        first_name,
                        last_name,
                        COALESCE(middle_name, ''),
                        COALESCE(ssn, '')
                );
            """)
            self.stdout.write("✅ Index created")

        duration = time.time() - start_time
        self.stdout.write(f"⏱  Done in {duration:.2f} seconds.")
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Imports phone numbers from PersonAddress to Phone and associates with Person"

    def handle(self, *args, **options):
        with connection.cursor() as cursor:

            self.stdout.write("1. Inserting unique numbers from PersonAddress to Phone...")

            cursor.execute("""
                INSERT INTO base_phone (phone_number)
                SELECT DISTINCT TRIM(phone)
                FROM base_personaddress
                WHERE phone IS NOT NULL AND TRIM(phone) <> ''
                ON CONFLICT DO NOTHING
            """)
            self.stdout.write("   ✅ Insert unique numbers is done.")

            self.stdout.write("2. Inserting Person ↔ Phone relationships (via person_home_addresses)...")

            cursor.execute("""
                INSERT INTO base_person_phones (person_id, phone_id)
                SELECT DISTINCT
                    pha.person_id,
                    ph.id
                FROM base_person_home_addresses pha
                JOIN base_personaddress pa ON pa.id = pha.personaddress_id
                JOIN base_phone ph ON TRIM(pa.phone) = ph.phone_number
                WHERE pa.phone IS NOT NULL AND TRIM(pa.phone) <> ''
                ON CONFLICT DO NOTHING
            """)
            self.stdout.write("   ✅ Done.")

        self.stdout.write(self.style.SUCCESS("Import is complete."))

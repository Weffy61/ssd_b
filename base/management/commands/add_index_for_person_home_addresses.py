import time
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Add expression index for base_personaddress to optimize M2M insert"
    requires_atomic = False

    def handle(self, *args, **options):
        start_time = time.time()

        index_name = "personaddress_address_city_zip_expr_idx"
        table_name = "base_personaddress"

        with connection.cursor() as cursor:
            self.stdout.write("🔍 Checking if index already exists...")

            cursor.execute("""
                SELECT 1
                FROM pg_indexes
                WHERE tablename = %s AND indexname = %s
            """, [table_name, index_name])

            if cursor.fetchone():
                self.stdout.write(f"✅ Index '{index_name}' already exists.")
            else:
                self.stdout.write(f"🚀 Creating index '{index_name}'...")

                cursor.execute(f"""
                    CREATE INDEX CONCURRENTLY {index_name}
                    ON {table_name} (
                        (COALESCE(address, '')),
                        (COALESCE(city, '')),
                        (COALESCE(state, '')),
                        (COALESCE(zip_code, '')),
                        (COALESCE(phone, ''))
                    );
                """)
                self.stdout.write(f"✅ Index '{index_name}' created.")

        duration = time.time() - start_time
        self.stdout.write(f"⏱ Done in {duration:.2f} seconds.")

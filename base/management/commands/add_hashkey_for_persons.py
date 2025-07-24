import hashlib
import time

from django.core.management.base import BaseCommand
from base.models import Person, AllPersonsDataAtt

BATCH_SIZE = 100_000


def make_hash_key(first_name, last_name, middle_name, ssn):
    key = f"{first_name or ''}|{last_name or ''}|{middle_name or ''}|{ssn or ''}"
    return hashlib.sha256(key.encode('utf-8')).hexdigest()


class Command(BaseCommand):
    help = "Generate hash_key for Person and AllPersonsDataAtt"

    def handle(self, *args, **options):
        start_time = time.time()
        self.stdout.write("📤 Generating hash_key for base_person...")
        self.update_person_hashes()

        self.stdout.write("📤 Generating hash_key for base_allpersonsdataatt...")
        self.update_att_hashes()

        end_time = time.time()
        total = end_time - start_time
        self.stdout.write(f"🎉 Finished in {total:.2f} seconds.")

    def update_person_hashes(self):
        qs = (
            Person.objects
            .filter(hash_key__isnull=True)
            .only("id", "first_name", "last_name", "middle_name", "ssn")
            .iterator(chunk_size=10_000)
        )

        batch = []
        total = 0
        for obj in qs:
            obj.hash_key = make_hash_key(obj.first_name, obj.last_name, obj.middle_name, obj.ssn)
            batch.append(obj)

            if len(batch) >= BATCH_SIZE:
                Person.objects.bulk_update(batch, ['hash_key'], batch_size=10_000)
                total += len(batch)
                self.stdout.write(f"Updated {total} Person records")
                batch = []

        if batch:
            Person.objects.bulk_update(batch, ['hash_key'], batch_size=10_000)
            total += len(batch)
            self.stdout.write(f"Updated {total} Person records")

    def update_att_hashes(self):
        qs = (
            AllPersonsDataAtt.objects
            .filter(hash_key__isnull=True)
            .only("id", "first_name", "last_name", "middle_name", "ssn")
            .iterator(chunk_size=10_000)
        )

        batch = []
        total = 0
        for obj in qs:
            obj.hash_key = make_hash_key(obj.first_name, obj.last_name, obj.middle_name, obj.ssn)
            batch.append(obj)

            if len(batch) >= BATCH_SIZE:
                AllPersonsDataAtt.objects.bulk_update(batch, ['hash_key'], batch_size=10_000)
                total += len(batch)
                self.stdout.write(f"Updated {total} AllPersonsDataAtt records")
                batch = []

        if batch:
            AllPersonsDataAtt.objects.bulk_update(batch, ['hash_key'], batch_size=10_000)
            total += len(batch)
            self.stdout.write(f"Updated {total} AllPersonsDataAtt records")


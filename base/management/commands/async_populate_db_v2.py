import asyncio
import time
from datetime import datetime
import aiofiles
from django.core.management.base import BaseCommand
from base.models import Person, PersonalData, PersonAddress
from asgiref.sync import sync_to_async


def parse_date(date_str):
    date_str = date_str.strip()
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        return None


class Command(BaseCommand):
    help = 'Import peoples data from text format to db'
    BATCH_SIZE = 100000  # Размер батча для обработки

    def add_arguments(self, parser):
        parser.add_argument('file_path', type=str, help='Path to the Peoples file')

    @sync_to_async
    def process_batch_sync(self, batch):
        persons = []
        personal_data_list = []
        addresses = []

        for row in batch:
            first_name = row.get('firstname', '').strip()
            last_name = row.get('lastname', '').strip()
            middle_name = row.get('middlename', '').strip() or None
            ssn = row.get('ssn', '').strip()

            persons.append(Person(
                first_name=first_name,
                last_name=last_name,
                middle_name=middle_name,
                ssn=ssn
            ))

            personal_data_fields = {
                'dob': parse_date(row.get('dob')),
                'name_suffix': row.get('name_suff', '').strip() or None,
                'alt1_dob': parse_date(row.get('alt1DOB')),
                'alt2_dob': parse_date(row.get('alt2DOB')),
                'alt3_dob': parse_date(row.get('alt3DOB')),
                'aka1_fullname': row.get('aka1fullname', '').strip() or None,
                'aka2_fullname': row.get('aka2fullname', '').strip() or None,
                'aka3_fullname': row.get('aka3fullname', '').strip() or None,
            }

            if any(value is not None for value in personal_data_fields.values()):
                personal_data_list.append((personal_data_fields.copy(), len(persons) - 1))

            addresses.append(PersonAddress(
                address=row.get('address', '').strip() or None,
                city=row.get('city', '').strip() or None,
                county=row.get('county_name', '').strip() or None,
                state=row.get('st', '').strip() or None,
                zip_code=row.get('zip', '').strip() or None,
                phone=row.get('phone1', '').strip() or None,
                start_date=parse_date(row.get("StartDat")),
            ))

        # Bulk create
        created_persons = Person.objects.bulk_create(persons)

        # Create PersonalData objects
        personal_data_objects = [
            PersonalData(person=created_persons[idx], **fields)
            for fields, idx in personal_data_list
        ]
        if personal_data_objects:
            PersonalData.objects.bulk_create(personal_data_objects)

        created_addresses = PersonAddress.objects.bulk_create(addresses)

        # Create relations using through model directly
        through_model = Person.home_addresses.through
        relations = [
            through_model(
                person_id=created_persons[i].id,
                personaddress_id=created_addresses[i].id
            )
            for i in range(len(created_persons))
        ]
        through_model.objects.bulk_create(relations)

    async def process_batch(self, batch):
        await self.process_batch_sync(batch)

    async def import_data(self, file_path):
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as file:
            headers = (await file.readline()).strip().split(',')
            batch = []

            async for line in file:
                parts = line.strip().split(',')

                if len(parts) == 20:
                    row = dict(zip(headers, parts))
                elif len(parts) == 19:
                    parts.insert(9, '')
                    row = dict(zip(headers, parts))
                elif len(parts) == 21:
                    parts.pop(15)
                    row = dict(zip(headers, parts))
                elif len(parts) == 22:
                    parts.pop(15)
                    parts.pop(16)
                    row = dict(zip(headers, parts))
                elif len(parts) == 23:
                    parts.pop(15)
                    parts.pop(16)
                    parts.pop(17)
                elif len(parts) == 24:
                    parts.pop(15)
                    parts.pop(16)
                    parts.pop(17)
                    parts.pop(18)
                    row = dict(zip(headers, parts))
                else:
                    self.stdout.write(self.style.ERROR(f'Error in line: {line} with len - {len(parts)}'))
                    continue

                batch.append(row)

                if len(batch) >= self.BATCH_SIZE:
                    await self.process_batch(batch)
                    batch = []
                    self.stdout.write(f"Processed {self.BATCH_SIZE} records")

            if batch:  # Обработать оставшиеся записи
                await self.process_batch(batch)
                self.stdout.write(f"Processed {len(batch)} records")

    def handle(self, *args, **kwargs):
        file_path = kwargs['file_path']
        loop = asyncio.get_event_loop()

        start = time.time()

        loop.run_until_complete(self.import_data(file_path))

        end = time.time()
        total = end - start
        print(f'finished in {total:.4f} second(s)')
        self.stdout.write(self.style.SUCCESS('Data imported successfully'))

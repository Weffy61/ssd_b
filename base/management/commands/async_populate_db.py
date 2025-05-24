import asyncio
import time
from datetime import datetime

import aiofiles
from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand
from base.models import Person, PersonalData, PersonAddress


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

    def add_arguments(self, parser):
        parser.add_argument('file_path', type=str, help='Path to the Peoples file')

    async def import_data(self, file_path):
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as file:
            lines = await file.readlines()
        headers = lines[0].strip().split(',')
        for line in lines[1:]:
            parts = line.strip().split(',')

            if len(parts) == 20:
                row = dict(zip(headers, parts))
            elif len(parts) == 19:
                parts.insert(9, '')
                row = dict(zip(headers, parts))
            elif len(parts) == 21:
                parts.pop(15)
                row = dict(zip(headers, parts))
            else:
                self.stdout.write(self.style.ERROR(f'Error in line: {line} with len - {len(parts)}'))
                continue

            first_name = row.get('firstname', '').strip()
            last_name = row.get('lastname', '').strip()
            middle_name = row.get('middlename', '').strip() or None
            ssn = row.get('ssn', '').strip()

            person, _ = await Person.objects.aget_or_create(
                first_name=first_name,
                last_name=last_name,
                middle_name=middle_name,
                ssn=ssn
            )

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
                await PersonalData.objects.acreate(
                    person=person,
                    **personal_data_fields
                )
            person_address, _ = await PersonAddress.objects.aget_or_create(
                address=row.get('address', '').strip() or None,
                city=row.get('city', '').strip() or None,
                county=row.get('county_name', '').strip() or None,
                state=row.get('st', '').strip() or None,
                zip_code=row.get('zip', '').strip() or None,
                phone=row.get('phone1', '').strip() or None,
                start_date=parse_date(row.get("StartDat"))
            )

            await sync_to_async(person.home_addresses.add)(person_address)
            current_row = int(row.get("ID", 0))
            if current_row % 10000 == 0:
                print(f'Added row {current_row}')

    def handle(self, *args, **kwargs):
        file_path = kwargs['file_path']
        loop = asyncio.get_event_loop()
        start = time.time()

        loop.run_until_complete(self.import_data(file_path))

        end = time.time()
        total = end - start
        print(f'finished in {total:.4f} second(s)')
        self.stdout.write(self.style.SUCCESS('Data imported successfully'))

from datetime import datetime
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

    def handle(self, *args, **kwargs):
        file_path = kwargs['file_path']
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        headers = lines[0].strip().split(',')
        for line in lines[1:]:
            parts = line.strip().split(',')

            if len(parts) == 20:
                row = dict(zip(headers, parts))
            elif len(parts) == 19:
                parts.insert(9, '')
                row = dict(zip(headers, parts))

            else:
                self.stdout.write(self.style.ERROR(f'Error in line: {line} with len - {len(line)}'))
                continue

            first_name = row.get('firstname', '').strip()
            last_name = row.get('lastname', '').strip()
            middle_name = row.get('middlename', '').strip() or None
            ssn = row.get('ssn', '').strip()

            person, _ = Person.objects.get_or_create(
                first_name=first_name,
                last_name=last_name,
                middle_name=middle_name,
                ssn=ssn
            )

            PersonalData.objects.create(
                person=person,
                dob=parse_date(row.get('dob')),
                name_suffix=row.get('name_suff', '').strip() or None,
                alt1_dob=parse_date(row.get('alt1DOB')),
                alt2_dob=parse_date(row.get('alt2DOB')),
                alt3_dob=parse_date(row.get('alt3DOB')),
                aka1_fullname=row.get('aka1fullname', '').strip() or None,
                aka2_fullname=row.get('aka2fullname', '').strip() or None,
                aka3_fullname=row.get('aka3fullname', '').strip() or None
            )

            person_address, _ = PersonAddress.objects.get_or_create(
                address=row.get('address', '').strip() or None,
                city=row.get('city', '').strip() or None,
                county=row.get('county_name', '').strip() or None,
                state=row.get('st', '').strip() or None,
                zip_code=row.get('zip', '').strip() or None,
                phone=row.get('phone1', '').strip() or None,
                start_date=parse_date(row.get("StartDat"))
            )

            person.home_addresses.add(person_address)
            self.stdout.write(self.style.SUCCESS(f'Added row {row.get("ID", "")}'))

        self.stdout.write(self.style.SUCCESS('Data imported successfully'))

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from django.db import utils
from django.core.management import BaseCommand

from base.models import PersonAddress, Person


@dataclass
class PersonDetails:
    first_name: Optional[str]
    last_name: Optional[str]
    middle_name: Optional[str]
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip_code: Optional[str]
    ssn: str
    dob: Optional[datetime.date]


class Command(BaseCommand):
    help = "Populate db new data from experian_ssn file"

    @staticmethod
    def parse_date(date_str: Optional[str]) -> Optional[datetime.date]:
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%m/%d/%Y", "%m/%d/%Y %I:%M:%S %p"):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def is_compatible_with_iso8859_5(s: str) -> bool:
        try:
            s.encode('iso-8859-5')
            return True
        except UnicodeEncodeError:
            return False

    def parse_person_detail(self, line: str) -> Optional[PersonDetails]:
        headers = ['first_middle_name', 'last_name', 'address', 'city', 'state', 'zip_code', 'ssn', 'dob']
        parts = line.strip().split(':', 7)
        if len(parts) != len(headers):
            return

        row = dict(zip(headers, parts))

        if not any(row.values()):
            return

        first_middle_name = row.get('first_middle_name', '').strip().split() or None
        if len(first_middle_name) > 1:
            first_name = first_middle_name[0][:100]
            middle_name = (' '.join(first_middle_name[1:]))[:100]

        else:
            first_name = first_middle_name[0][:100]
            middle_name = None

        last_name = row.get('last_name', '').strip()[:100] or None
        address = row.get('address', '').strip() or None
        city = row.get('city', '').strip()[:100] or None
        state = row.get('state', '').strip()[:100] or None
        zip_code = row.get('zip_code', '').strip()[:50] or None
        ssn = row.get('ssn', '').strip().replace('-', '')[:10] or None
        dob = self.parse_date(row.get('dob'))
        person = PersonDetails(
            first_name=first_name,
            last_name=last_name,
            middle_name=middle_name,
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            ssn=ssn,
            dob=dob
        )
        return person

    def add_arguments(self, parser):
        parser.add_argument('file_path', type=str, help='Path to the Peoples file')

    def add_person_details_to_db(self, person_details: PersonDetails) -> None:
        try:
            full_address, a_created = PersonAddress.objects.get_or_create(
                address=person_details.address,
                city=person_details.city,
                county=None,
                state=person_details.state,
                zip_code=person_details.zip_code,
                phone=None
            )

            person, p_created = Person.objects.get_or_create(
                first_name=person_details.first_name,
                ssn=person_details.ssn,
                last_name=person_details.last_name,
                middle_name=person_details.middle_name
            )

            if a_created or not person.home_addresses.filter(id=full_address.id).exists():
                person.home_addresses.add(full_address)
            if p_created:
                person.personal_datas.create(dob=person_details.dob)
            else:
                personal_datas = person.personal_datas.all()

                dobs = [
                    dob
                    for pd in personal_datas
                    for dob in (pd.dob, pd.alt1_dob, pd.alt2_dob, pd.alt3_dob)
                    if dob is not None
                ]

                if person_details.dob not in dobs:
                    person.personal_datas.create(dob=person_details.dob)
        except utils.DataError:
            return

    def handle(self, *args, **options):
        start_time = time.time()
        self.stdout.write(f"📊 Processing populate data...")
        path = options['file_path']
        with open(path, 'r', encoding='utf-8', errors='ignore', buffering=1024 * 1024) as f:
            line_num = 0

            for line in f:
                line_num += 1
                if not self.is_compatible_with_iso8859_5(line):
                    self.stdout.write(f"⚠️ Line {line_num} is not ISO_8859_5 compatible — skipped.")
                    continue

                parsed_person = self.parse_person_detail(line)
                if not parsed_person:
                    continue

                self.add_person_details_to_db(parsed_person)
                if line_num % 1000 == 0:
                    self.stdout.write(f"✅ Processed {line_num} lines.")
        total_time = time.time() - start_time
        self.stdout.write(f"🎉 Full populating finished in {total_time:.2f} seconds.")

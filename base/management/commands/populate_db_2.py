import argparse
import os
import time

import psycopg2
import tempfile
from datetime import datetime


def parse_date(date_str):
    date_str = date_str.strip()
    if not date_str:
        return ''
    try:
        return datetime.strptime(date_str, "%Y%m%d").strftime('%Y-%m-%d')
    except ValueError:
        return ''


def connect_db():
    return psycopg2.connect(
        dbname="s4ndman",
        user="admin",
        password="root",
        host="localhost",
        port="5432"
    )


def import_data(file_path, batch_size=1000000):
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, buffering=1024*1024) as person_file, \
            tempfile.NamedTemporaryFile(mode='w+', delete=False, buffering=1024*1024) as personal_data_file, \
            tempfile.NamedTemporaryFile(mode='w+', delete=False, buffering=1024*1024) as address_file, \
            tempfile.NamedTemporaryFile(mode='w+', delete=False, buffering=1024*1024) as person_address_m2m_file:

        person_filename = person_file.name
        personal_data_filename = personal_data_file.name
        address_filename = address_file.name
        person_address_m2m_filename = person_address_m2m_file.name

        person_id_map = {}
        address_id_map = {}
        person_address_pairs = set()

        with open(file_path, 'r', encoding='utf-8', buffering=1024*1024) as f:
            headers = f.readline().strip().split(',')
            line_num = 0
            batch_counter = 0

            for line in f:
                line_num += 1
                batch_counter += 1
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
                    print(f'Error in line {line_num}: {line} with len - {len(parts)}')
                    continue

                first_name = row.get('firstname', '').strip().replace('\t', ' ')
                last_name = row.get('lastname', '').strip().replace('\t', ' ')
                middle_name = (row.get('middlename', '').strip() or '').replace('\t', ' ')
                ssn = row.get('ssn', '').strip()

                person_key = (first_name, last_name, middle_name, ssn)
                if person_key not in person_id_map:
                    person_id = len(person_id_map) + 1
                    person_id_map[person_key] = person_id
                    person_file.write(f"{person_id}\t{first_name}\t{last_name}\t{middle_name}\t{ssn}\n")

                person_id = person_id_map[person_key]
                dob = parse_date(row.get('dob')) or ''
                name_suffix = (row.get('name_suff', '').strip() or '').replace('\t', ' ')
                alt1_dob = parse_date(row.get('alt1DOB')) or ''
                alt2_dob = parse_date(row.get('alt2DOB')) or ''
                alt3_dob = parse_date(row.get('alt3DOB')) or ''
                aka1_fullname = (row.get('aka1fullname', '').strip() or '').replace('\t', ' ')
                aka2_fullname = (row.get('aka2fullname', '').strip() or '').replace('\t', ' ')
                aka3_fullname = (row.get('aka3fullname', '').strip() or '').replace('\t', ' ')

                if any([dob, name_suffix, alt1_dob, alt2_dob, alt3_dob,
                        aka1_fullname, aka2_fullname, aka3_fullname]):
                    personal_data_file.write(
                        f"{person_id}\t{dob}\t{name_suffix}\t{alt1_dob}\t{alt2_dob}\t{alt3_dob}\t"
                        f"{aka1_fullname}\t{aka2_fullname}\t{aka3_fullname}\n"
                    )

                address_key = (
                    (row.get('address', '').strip() or '').replace('\t', ' '),
                    (row.get('city', '').strip() or '').replace('\t', ' '),
                    (row.get('county_name', '').strip() or '').replace('\t', ' '),
                    (row.get('st', '').strip() or '').replace('\t', ' '),
                    (row.get('zip', '').strip() or '').replace('\t', ' '),
                    (row.get('phone1', '').strip() or '').replace('\t', ' '),
                )

                if address_key not in address_id_map:
                    address_id = len(address_id_map) + 1
                    address_id_map[address_key] = address_id
                    address_file.write(
                        f"{address_id}\t{address_key[0]}\t{address_key[1]}\t{address_key[2]}\t"
                        f"{address_key[3]}\t{address_key[4]}\t{address_key[5]}\n"
                    )

                pair = (person_id, address_id_map[address_key])
                if pair not in person_address_pairs:
                    person_address_pairs.add(pair)
                    person_address_m2m_file.write(f"{person_id}\t{address_id_map[address_key]}\n")

                if batch_counter >= batch_size:
                    print(f"Processed {line_num} rows...")
                    person_file.flush()
                    personal_data_file.flush()
                    address_file.flush()
                    person_address_m2m_file.flush()
                    batch_counter = 0

        print("Finished processing file. Starting COPY commands...")

        conn = connect_db()
        conn.autocommit = False
        cursor = conn.cursor()

        try:
            cursor.execute("SET synchronous_commit TO off;")
            cursor.execute("SET maintenance_work_mem TO '1GB';")
            cursor.execute("SET work_mem TO '256MB';")
            # cursor.execute("SET checkpoint_timeout TO '1h';")

            cursor.execute("ALTER TABLE base_person_home_addresses SET UNLOGGED;")
            cursor.execute("ALTER TABLE base_personaldata SET UNLOGGED;")
            cursor.execute("ALTER TABLE base_personaddress SET UNLOGGED;")
            cursor.execute("ALTER TABLE base_person SET UNLOGGED;")



            cursor.execute("ALTER TABLE base_person DISABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_personaldata DISABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_personaddress DISABLE TRIGGER USER;")


            print("Copying Person data...")
            with open(person_filename, 'r') as f:
                cursor.copy_expert(
                    "COPY base_person (id, first_name, last_name, middle_name, ssn) FROM STDIN WITH DELIMITER '\t' NULL ''",
                    f
                )

            print("Copying Address data...")
            with open(address_filename, 'r') as f:
                cursor.copy_expert(
                    "COPY base_personaddress (id, address, city, county, state, zip_code, phone) FROM STDIN WITH DELIMITER '\t' NULL ''",
                    f
                )

            print("Copying PersonalData data...")
            with open(personal_data_filename, 'r') as f:
                cursor.copy_expert(
                    """COPY base_personaldata (person_id, dob, name_suffix, alt1_dob, alt2_dob, alt3_dob, 
                    aka1_fullname, aka2_fullname, aka3_fullname) FROM STDIN WITH DELIMITER '\t' NULL ''""",
                    f
                )

            conn.commit()

            conn.autocommit = False

            print("Copying M2M relations...")
            with open(person_address_m2m_filename, 'r') as f:
                cursor.copy_expert(
                    "COPY base_person_home_addresses (person_id, personaddress_id) FROM STDIN WITH DELIMITER '\t'",
                    f
                )

            cursor.execute("ALTER TABLE base_person SET LOGGED;")
            cursor.execute("ALTER TABLE base_personaddress SET LOGGED;")
            cursor.execute("ALTER TABLE base_personaldata SET LOGGED;")
            cursor.execute("ALTER TABLE base_person_home_addresses SET LOGGED;")

            cursor.execute("ALTER TABLE base_person ENABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_personaldata ENABLE TRIGGER USER;")
            cursor.execute("ALTER TABLE base_personaddress ENABLE TRIGGER USER;")


            cursor.execute("""
                SET LOCAL maintenance_work_mem TO '2GB';
                SELECT setval('base_person_id_seq', 
                       (SELECT MAX(id) FROM base_person),
                       false);
                SELECT setval('base_personaddress_id_seq', 
                       (SELECT MAX(id) FROM base_personaddress),
                       false);
            """)
            # cursor.execute("SELECT setval('base_person_id_seq', (SELECT MAX(id) FROM base_person));")
            # cursor.execute("SELECT setval('base_personaddress_id_seq', (SELECT MAX(id) FROM base_personaddress));")

            conn.commit()
            print("Data imported successfully")

        except Exception as e:
            conn.rollback()
            raise e
            # print(f"Error: {e}")
        finally:
            cursor.close()
            conn.close()

        os.unlink(person_filename)
        os.unlink(personal_data_filename)
        os.unlink(address_filename)
        os.unlink(person_address_m2m_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import peoples data using COPY')
    parser.add_argument('file_path', type=str, help='Path to the Peoples file')
    parser.add_argument('--batch-size', type=int, default=100000, help='Batch size for processing')

    args = parser.parse_args()

    start = time.time()
    import_data(args.file_path, args.batch_size)

    end = time.time()
    total = end - start
    print(f'finished in {total:.4f} second(s)')
import argparse
import os
import time
import psycopg2
import tempfile
from datetime import datetime

EXPECTED_LENGTH = 20
INSERT_INDEX = 9
TRIM_START_INDEX = 15


def parse_date(date_str):
    date_str = date_str.strip()
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y%m%d").strftime('%Y-%m-%d')
    except ValueError:
        return None


def connect_db():
    return psycopg2.connect(
        dbname="django_db",
        user="admin",
        password="root",
        host="localhost",
        port="5432"
    )


def import_data(file_path, batch_size):
    try:
        print('Checking database')
        conn = connect_db()
        conn.close()
        print('Database connection successful')
    except Exception as e:
        print(f'Database connection failed: {e}')
        return
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, buffering=1024*1024) as temp_file:
        temp_filename = temp_file.name

        with open(file_path, 'r', encoding='utf-8', errors='ignore', buffering=1024*1024) as f:
            headers = f.readline().strip().split(',')
            line_num = 0
            batch_counter = 0

            for line in f:
                line_num += 1
                batch_counter += 1
                parts = line.strip().split(',')

                if len(parts) == EXPECTED_LENGTH:
                    pass
                elif len(parts) == EXPECTED_LENGTH - 1:
                    parts.insert(INSERT_INDEX, '')
                elif len(parts) > EXPECTED_LENGTH:
                    while len(parts) > EXPECTED_LENGTH:
                        parts.pop(TRIM_START_INDEX)
                else:
                    print(f'Error in line {line_num}: {line} with len - {len(parts)}')
                    continue

                row = dict(zip(headers, parts))
                first_name = row.get('firstname', '').strip().replace('\t', ' ')[:100] or None
                last_name = row.get('lastname', '').strip().replace('\t', ' ')[:100] or None
                middle_name = (row.get('middlename', '').strip() or '')[:100].replace('\t', ' ') or None
                ssn = (row.get('ssn', '').strip() or '')[:10] or None
                dob = parse_date(row.get('dob'))
                name_suffix = (row.get('name_suff', '').strip() or '')[:100].replace('\t', ' ') or None
                alt1_dob = parse_date(row.get('alt1DOB'))
                alt2_dob = parse_date(row.get('alt2DOB'))
                alt3_dob = parse_date(row.get('alt3DOB'))
                aka1_fullname = (row.get('aka1fullname', '').strip() or '')[:200].replace('\t', ' ') or None
                aka2_fullname = (row.get('aka2fullname', '').strip() or '')[:200].replace('\t', ' ') or None
                aka3_fullname = (row.get('aka3fullname', '').strip() or '')[:200].replace('\t', ' ') or None
                address = (row.get('address', '').strip() or '').replace('\t', ' ') or None
                city = (row.get('city', '').strip() or '')[:100].replace('\t', ' ') or None
                county = (row.get('county_name', '').strip() or '')[:100].replace('\t', ' ') or None
                state = (row.get('st', '').strip() or '')[:100].replace('\t', ' ') or None
                zip_code = (row.get('zip', '').strip() or '')[:50].replace('\t', ' ') or None
                phone = (row.get('phone1', '').strip() or '')[:50].replace('\t', ' ') or None
                start_date = None

                def format_value(val):
                    return '\\N' if val is None else str(val)

                temp_file.write(
                    f"{format_value(first_name)}\t{format_value(last_name)}\t{format_value(middle_name)}\t"
                    f"{format_value(ssn)}\t{format_value(dob)}\t{format_value(name_suffix)}\t"
                    f"{format_value(alt1_dob)}\t{format_value(alt2_dob)}\t{format_value(alt3_dob)}\t"
                    f"{format_value(aka1_fullname)}\t{format_value(aka2_fullname)}\t"
                    f"{format_value(aka3_fullname)}\t{format_value(address)}\t"
                    f"{format_value(city)}\t{format_value(county)}\t{format_value(state)}\t"
                    f"{format_value(zip_code)}\t{format_value(phone)}\t{format_value(start_date)}\n"
                )

                if batch_counter >= batch_size:
                    print(f"Processed {line_num} rows...")
                    temp_file.flush()
                    batch_counter = 0

        print("Finished processing file. Starting COPY command...")

        conn = connect_db()
        conn.autocommit = False
        cursor = conn.cursor()

        try:
            cursor.execute("SET synchronous_commit TO off;")
            cursor.execute("SET maintenance_work_mem TO '1GB';")
            cursor.execute("SET work_mem TO '256MB';")
            cursor.execute("ALTER TABLE base_allpersonsdata SET UNLOGGED;")
            cursor.execute("ALTER TABLE base_allpersonsdata DISABLE TRIGGER USER;")

            print("Copying AllPersonsData data...")
            with open(temp_filename, 'r') as f:
                cursor.copy_expert(
                    """COPY base_allpersonsdata (
                        first_name, last_name, middle_name, ssn, dob, name_suffix,
                        alt1_dob, alt2_dob, alt3_dob, aka1_fullname, aka2_fullname,
                        aka3_fullname, address, city, county, state, zip_code, phone, start_date
                    ) FROM STDIN WITH DELIMITER '\t' NULL '\\N'""",
                    f
                )

            cursor.execute("ALTER TABLE base_allpersonsdata SET LOGGED;")
            cursor.execute("ALTER TABLE base_allpersonsdata ENABLE TRIGGER USER;")

            conn.commit()
            print("Data imported successfully")

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

        os.unlink(temp_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import peoples data into base_allpersonsdata using PostgreSQL COPY')
    parser.add_argument('file_path', type=str, help='Path to the Peoples file')
    parser.add_argument('--batch-size', type=int, default=1000000, help='Batch size for processing')

    args = parser.parse_args()

    start = time.time()
    import_data(args.file_path, args.batch_size)

    end = time.time()
    total = end - start
    print(f'Finished in {total:.4f} second(s)')
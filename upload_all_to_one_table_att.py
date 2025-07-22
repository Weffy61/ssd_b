import argparse
import os
import re
import tempfile
import time
from datetime import datetime
from typing import Optional, List

import psycopg2

TITLE_ABBREVIATIONS = ['MR.', 'MRS.', 'MS.', 'MISS.', 'MX.', 'MR', 'MRS', 'MS', 'MISS', 'MX']

ADDRESS_ABBREVIATIONS = [
    "APT", "BLDG", "DEPT", "FL", "FRNT", "HNGR", "LBBY", "LOT", "LOT", "LOWR", "OFC", "PH",
    "PIER", "REAR", "RM", "SIDE", "SLIP", "SPC", "STOP", "STE", "TRLR", "UNIT", "UPPR"]

US_STATE_ABBREVIATIONS = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC"
]


def extract_us_state_abbreviation(us_state: str) -> Optional[str]:
    pattern = r'\b(' + '|'.join(US_STATE_ABBREVIATIONS) + r')\b'
    match = re.search(pattern, us_state, flags=re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None


def extract_zip_code(text: str) -> Optional[str]:
    match = re.search(r'\b\d{5}(?:-\d{4})?\b', text)
    return match.group(0) if match else None


def contains_address_abbreviation(address_part: str) -> bool:
    pattern = r'\b(?:' + '|'.join(ADDRESS_ABBREVIATIONS) + r')\b'
    return bool(re.search(pattern, address_part, flags=re.IGNORECASE))


def is_valid_fname(f_name: str) -> bool:
    if f_name.upper().strip() in TITLE_ABBREVIATIONS:
        return False
    return True


def validate_p_num(p_number: str) -> Optional[str]:
    return p_number if p_number.isdigit() else None


def validate_ssn(ssn_num: str) -> Optional[str]:
    match = re.search(r'\b\d{3}-\d{2}-\d{4}\b|\b\d{9}\b', ssn_num)
    if match:
        return match.group(0).replace('-', '')
    return None


def validate_date(text: str) -> Optional[str]:
    match = re.search(r'\b\d{4}-\d{2}-\d{2}\b', text)
    if match:
        date_str = match.group(0)
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        except ValueError:
            return None
    return None


def clean_line(line: str) -> List:
    parts = line.strip().split('\"')
    return ''.join(parts).strip().replace('*', '').split(',')


def parse_name(full_name: str) -> tuple:
    name_parts = full_name.split()
    if not is_valid_fname(name_parts[0]):
        name_parts.pop(0)
    first = name_parts[0].strip()
    if len(name_parts) == 2:
        last = name_parts[-1].strip()
        return first, last, None
    elif len(name_parts) > 2:
        last = name_parts[-1].strip()
        middle = ' '.join(name_parts[1:-1]).strip()
        return first, last, middle
    return first, None, None


def extract_address_info(parts: list) -> tuple:
    address = city = state = zip_code = None
    addr_2 = ''
    full_address = parts[6:]
    if len(parts) == 8:
        addr_1 = full_address[0].strip()
        last_part = full_address[1].strip()
        zip_code = extract_zip_code(last_part)
        if zip_code:
            last_part = last_part.replace(zip_code, '')
        last_words = last_part.strip().split()
        if extract_us_state_abbreviation(last_words[-1]):
            state = last_part.split()[-1].strip()
            last_part = last_part.replace(state, '')
        else:
            return None, None, None, None
        city = last_part.strip()
    elif len(parts) == 9:
        if contains_address_abbreviation(full_address[0]):
            addr_2 = full_address[0].strip()
            addr_1 = full_address[1].strip()
        else:
            addr_1 = full_address[0].strip()
            middle_part = full_address[1].strip()
            last_part = full_address[2].strip()

            zip_code = extract_zip_code(last_part)
            if not zip_code:
                zip_code = extract_zip_code(middle_part)

            last_part = full_address[2].strip().split()
            if extract_us_state_abbreviation(last_part[-1].strip()):
                state = last_part[-1].strip()
                last_part = ','.join(last_part).replace(state, '').split(',')
                city = ' '.join(last_part).strip()
            else:
                return None, None, None, None

    elif len(parts) == 10:
        if contains_address_abbreviation(full_address[0].split()[0]):
            addr_1 = full_address[1].strip()
            addr_2 = full_address[0].strip()

            middle_part = full_address[2].strip()
            last_part = full_address[3].strip()
            zip_code = extract_zip_code(last_part)
            if zip_code:
                last_part = last_part.replace(zip_code, '')
            elif extract_zip_code(middle_part):
                zip_code = extract_zip_code(middle_part)
            last_part = last_part.split()
            if extract_us_state_abbreviation(last_part[-1].strip()):
                state = last_part[-1].strip()
                last_part = ','.join(last_part).replace(state, '').split(',')
                city = ' '.join(last_part).strip()
            else:
                return None, None, None, None
        elif contains_address_abbreviation(full_address[1].split()[0]):
            addr_1 = full_address[2]
            addr_2 = f'{full_address[1]}, {full_address[0]}'

            last_part = full_address[-1].strip()
            zip_code = extract_zip_code(last_part)
            if zip_code:
                last_part = last_part.replace(zip_code, '')

            last_part = list(dict.fromkeys(last_part.split()))

            if extract_us_state_abbreviation(last_part[-1]):
                state = last_part[-1].strip()
                last_part.remove(state)
                city = ' '.join(last_part).strip()

            else:
                return None, None, None, None
        else:
            return None, None, None, None
    else:
        return None, None, None, None
    address = f'{addr_1}, {addr_2}' if addr_2 else addr_1

    return address, city, state, zip_code


def format_value(val):
    return '\\N' if val is None else str(val)


def safe_trim(val, max_len):
    return val[:max_len] if val else None


def connect_db():
    return psycopg2.connect(
        dbname="django_db",
        user="s4ndman",
        password="wweraw",
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

        with open(file_path, 'r', encoding='utf-8', errors='ignore', buffering=1024 * 1024) as f:
            headers = f.readline().strip().split(',')
            line_num = 0
            batch_counter = 0
            for line in f:
                line_num += 1
                try:
                    parts = clean_line(line)
                    if len(parts) <= 6:
                        continue
                    first_name, last_name, middle_name = parse_name(parts[0])
                    if not last_name:
                        continue

                    phone_1, phone_2 = validate_p_num(parts[1]), validate_p_num(parts[2])
                    ssn = validate_ssn(parts[3])
                    dob = validate_date(parts[4])
                    email = parts[5].strip()
                    address, city, state, zip_code = extract_address_info(parts)
                    if state is not None and len(state) != 2:
                        continue
                    if all(x is None for x in [address, city, state, zip_code]):
                        continue
                    first_name = safe_trim(first_name, 100)
                    last_name = safe_trim(last_name, 100)
                    middle_name = safe_trim(middle_name, 100)
                    ssn = safe_trim(ssn, 10)
                    dob = dob if dob else None
                    phone_1 = safe_trim(phone_1, 15)
                    phone_2 = safe_trim(phone_2, 15)
                    email = safe_trim(email, 250)
                    address = safe_trim(address, 300)
                    city = safe_trim(city, 100)
                    state = safe_trim(state, 100)
                    zip_code = safe_trim(zip_code, 100)

                    temp_file.write(
                        f"{format_value(first_name)}\t{format_value(last_name)}\t{format_value(middle_name)}\t"
                        f"{format_value(ssn)}\t{format_value(dob)}\t{format_value(address)}\t"
                        f"{format_value(city)}\t{format_value(state)}\t{format_value(zip_code)}\t"
                        f"{format_value(phone_1)}\t{format_value(phone_2)}\t{format_value(email)}\n"
                    )
                    if batch_counter >= batch_size:
                        print(f"Processed {line_num} rows...")
                        temp_file.flush()
                        batch_counter = 0

                except IndexError:
                    continue
                except Exception as e:
                    print(f'Error on line {line_num}: {e}')
                    continue
                except KeyboardInterrupt:
                    print(line_num)
                    break

    print("Finished processing file. Starting COPY command...")
    conn = connect_db()
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        cursor.execute("SET synchronous_commit TO off;")
        cursor.execute("SET maintenance_work_mem TO '1GB';")
        cursor.execute("SET work_mem TO '256MB';")
        cursor.execute("ALTER TABLE base_allpersonsdataatt SET UNLOGGED;")
        cursor.execute("ALTER TABLE base_allpersonsdataatt DISABLE TRIGGER USER;")

        print("Copying AllPersonsDataAtt data...")
        with open(temp_filename, 'r') as f:
            cursor.copy_expert(
                """COPY base_allpersonsdataatt (
                    first_name, last_name, middle_name, ssn, dob, address, 
                    city, state, zip_code, phone_1, phone_2, email
                ) FROM STDIN WITH DELIMITER '\t' NULL '\\N'""",
                f
            )

        cursor.execute("ALTER TABLE base_allpersonsdataatt SET LOGGED;")
        cursor.execute("ALTER TABLE base_allpersonsdataatt ENABLE TRIGGER USER;")

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
    parser = argparse.ArgumentParser(description='Import peoples data into base_allpersonsdataatt using PostgreSQL COPY')
    parser.add_argument('file_path', type=str, help='Path to the Peoples file')
    parser.add_argument('--batch-size', type=int, default=1000000, help='Batch size for processing')

    args = parser.parse_args()

    start = time.time()
    import_data(args.file_path, args.batch_size)

    end = time.time()
    total = end - start
    print(f'Finished in {total:.4f} second(s)')

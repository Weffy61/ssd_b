import re


def extract_house_number(address: str) -> str:
    match = re.match(r'\d+', address.strip())
    return match.group() if match else ''

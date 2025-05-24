from dataclasses import dataclass


@dataclass
class PersonSearch:
    first_name: str
    last_name: str
    address: str
    city: str
    state: str
    zip_code: str

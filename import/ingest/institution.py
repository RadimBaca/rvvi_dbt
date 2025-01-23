import os

import pandas as pd
import requests
from pydantic import BaseModel, field_validator
from datetime import datetime
from ingest.convert_helpers import truncate_string

class Institution(BaseModel):
    name: str
    ico: int
    street: str
    psc: int
    town: str
    legal_form: str
    main_goal: str
    created: datetime

    @field_validator('name', 'street', 'town', 'legal_form', 'main_goal', mode='before')
    def truncate_fields(cls, value, info):
        max_lengths = {
            'name': 1000,
            'street': 500,
            'town': 200,
            'legal_form': 500,
            'main_goal': 2000
        }
        if info.field_name in max_lengths:
            return truncate_string(value, max_lengths[info.field_name])
        return value

    @field_validator('psc', mode='before')
    def remove_spaces_from_psc(cls, value):
        return int(str(value).replace(" ", ""))

    @field_validator('created', mode='before')
    def parse_created_date(cls, value):
        return pd.to_datetime(value, format='%m/%d/%Y')

def build_institution_insert_sql(schema: str, institution: Institution) -> str:
    table_name = f"{schema}.institution"
    columns = "name, ico, street, psc, town, legal_form, main_goal, created"
    placeholders = ", ".join(["?"] * 8)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    return sql


def import_institutions(df, schema, conn, cursor):
    for _, row in df.iterrows():
        institution = Institution(
            name=row['Nazev_vyzkumne_organizace'],
            ico=row['ICO'],
            street=row['Sidlo'],
            psc=row['PSC'],
            town=row['Mesto'],
            legal_form=row['Pravni_forma'],
            main_goal=row['Hlavni_cil_cinnosti'],
            created=row['Datum_zapisu']
        )
        sql = build_institution_insert_sql(schema, institution)
        cursor.execute(sql, (
            institution.name,
            institution.ico,
            institution.street,
            institution.psc,
            institution.town,
            institution.legal_form,
            institution.main_goal,
            institution.created
        ))
    conn.commit()


def download_institutions():
    # Define the URL for the XLSX file and the local file path
    url = 'https://msmt.gov.cz/file/63699_1_1/'
    file_path = 'Seznam_vyzkumnych_organizaci-14.xlsx'
    # Download the file if it does not already exist
    if not os.path.exists(file_path):
        print(f"Downloading file from {url}...")
        response = requests.get(url)
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"File downloaded and saved to {file_path}")
            return file_path
        else:
            print(f"Failed to download the file. HTTP Status Code: {response.status_code}")
            exit(1)
    return file_path
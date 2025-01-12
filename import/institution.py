import os

import pandas as pd
import requests

from convert_helpers import truncate_string


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


def import_institutions(df, schema, conn, cursor):
    for index, row in df.iterrows():
        name = truncate_string(str(row['Nazev_vyzkumne_organizace']), 1000)
        ico = int(row['ICO'])
        street = truncate_string(str(row['Sidlo']), 500)
        psc = int(row['PSC'].replace(" ", ""))
        town = truncate_string(str(row['Mesto']), 200)
        legal_form = truncate_string(str(row['Pravni_forma']), 500)
        main_goal = truncate_string(str(row['Hlavni_cil_cinnosti']), 2000)
        created = pd.to_datetime(row['Datum_zapisu'], format='%m/%d/%Y')

        sql = f"""
        INSERT INTO {schema}.institution (name, ico, street, psc, town, legal_form, main_goal, created)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
        cursor.execute(sql, name, ico, street, psc, town, legal_form, main_goal, created)
    conn.commit()

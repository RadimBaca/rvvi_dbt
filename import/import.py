import os
import pandas as pd
import pyodbc
import requests
from dotenv import load_dotenv
import re
import zipfile

def db_connection():
    load_dotenv()
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    if not all([server, database, username, password]):
        raise ValueError("Database connection details are not fully provided in the .env file.")
    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    # Connect to the SQL Server database
    conn = pyodbc.connect(connection_string)
    return conn


def download_institutions():
    global url, file_path, response, f
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

def download_articles():
    urls = [
        ("https://hodnoceni.rvvi.cz/public/h23/H23%20M2%20biblio%20obory/1.%20Natural%20Sciences.zip", "1. Natural Sciences.zip"),
        ("https://hodnoceni.rvvi.cz/public//h23/H23%20M2%20biblio%20obory/2.%20Engineering%20and%20Technology.zip","2. Engineering and Technology.zip"),
        ("https://hodnoceni.rvvi.cz/public/h23/H23%20M2%20biblio%20obory/3.%20Medical%20and%20Health%20Sciences.zip","3. Medical and Health Sciences.zip"),
        ("https://hodnoceni.rvvi.cz/public/h23/H23%20M2%20biblio%20obory/4.%20Agricultural%20and%20Veterinary%20Sciences.zip","4. Agricultural and Veterinary Sciences.zip"),
        ("https://hodnoceni.rvvi.cz/public/h23/H23%20M2%20biblio%20obory/5.%20Social%20Sciences.zip","5. Social Sciences.zip"),
        ("https://hodnoceni.rvvi.cz/public/h23/H23%20M2%20biblio%20obory/6.%20Humanities%20and%20the%20Arts.zip", "6. Humanities and Arts.zip"),
    ]
    output_dir = "."

    for i, (url, zip_filename) in enumerate(urls, start=1):
        try:
            response = requests.get(url, stream=True, verify=False)
            response.raise_for_status()  # Raise an error for bad responses

            with open(zip_filename, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            extract_dir = os.path.splitext(zip_filename)[0]
            os.makedirs(extract_dir, exist_ok=True)

            with zipfile.ZipFile(zip_filename, "r") as zip_ref:
                zip_ref.extractall(extract_dir)

            os.remove(zip_filename)

        except Exception as e:
            print(f"Failed to process {url}: {e}")


    print(f"All files downloaded and extracted to '{output_dir}'.")


def create_rvvi_script(conn, cursor):
    with open('create_rvvi.sql', 'r') as file:
        sql_script = file.read()

    commands = sql_script.split('go\n')

    # Execute each command separately
    try:
        for command in commands:
            command = command.strip()
            if command:  # Skip empty commands
                cursor.execute(command)
                conn.commit()
        print("create_rvvi.sql SQL script executed successfully.")
    except Exception as e:
        print(f"Error executing SQL script: {e}")


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

def import_articles(schema, con, cursor):
    base_dir = '.'
    # Scan directories and process each XLSX file
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.startswith('Priloha_2_casopisy_') and file.endswith('.xlsx'):
                file_path = os.path.join(root, file)
                print(f'Processing journal file: {file_path}')

                # Extract the field of study and FORD field information from the directory structure
                parts = root.split(os.sep)
                field_of_study_name = strip_prefix(parts[-2])
                ford_field_name = parts[-1]

                # Derive the fid from the prefix pair in the FORD field directory name
                fid_match = re.match(r'^(\d+)\.(\d+)', ford_field_name)
                # print(ford_field_name + " - "+ str(fid_match))
                if fid_match:
                    fid = int(fid_match.group(1)) * 10 + int(fid_match.group(2))
                else:
                    print(f"Could not derive fid from FORD field '{ford_field_name}'")
                    continue

                # Verify if the field of study exists in the database
                cursor.execute(f"SELECT sid FROM {schema}.field_of_study WHERE name = ?", field_of_study_name)
                sid_row = cursor.fetchone()
                if not sid_row:
                    print(f"Field of study '{field_of_study_name}' not found in the database.")
                    continue
                sid = sid_row[0]

                # Insert the fields_ford entry if it does not exist
                cursor.execute(f"SELECT fid FROM {schema}.fields_ford WHERE fid = ?", fid)
                if not cursor.fetchone():
                    cursor.execute(f"INSERT INTO {schema}.fields_ford (fid, sid, name) VALUES (?, ?, ?)", fid, sid,
                                   ford_field_name)
                    conn.commit()

                # Read the XLS file
                xls = pd.ExcelFile(file_path)
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet_name)

                    for index, row in df.iterrows():
                        year = int(row['Rok uplatnění'])
                        name = str(row['Název'])
                        issn = str(row['ISSN']) if pd.notna(row['ISSN']) else ''
                        eissn = str(row['E-ISSN']) if pd.notna(row['E-ISSN']) else ''
                        article_count = int(row['Počet dokumentů']) if pd.notna(row['Počet dokumentů']) else 'NULL'
                        zone = str(row['Pásmo'])
                        czech_or_slovak = str(row['Český nebo slovenský časopis'])

                        # Truncate the ISSN and E-ISSN to fit within the maximum length 10
                        if len(issn) > 10:
                            issn = issn[:10]
                        # print('year:{}, name:{}, issn:{}, eissn:{}, article_count:{}, zone:{}, czech_or_slovak:{}, fid:{}'.format(year, name, issn, eissn, article_count, zone, czech_or_slovak, fid))

                        # Construct the SQL statement
                        sql = f"""
                            INSERT INTO {schema}.journal (year, name, issn, eissn, article_count, zone, czech_or_slovak, fid)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        cursor.execute(sql, year, name, issn, eissn, article_count, zone, czech_or_slovak, fid)

                    conn.commit()
                    print(f"Inserted data from sheet '{sheet_name}' into the database.")

            elif file.startswith('Priloha_3_vysledky_') and file.endswith('.xlsx'):
                file_path = os.path.join(root, file)
                print(f'Processing article file: {file_path}')

                # Extract the field of study and FORD field information from the directory structure
                parts = root.split(os.sep)
                field_of_study_name = strip_prefix(parts[-2])
                ford_field_name = parts[-1]

                # Derive the fid from the prefix pair in the FORD field directory name
                fid_match = re.match(r'^(\d+)\.(\d+)', ford_field_name)
                if fid_match:
                    fid = int(fid_match.group(1)) * 10 + int(fid_match.group(2))
                    ford_num = fid_match.group(1) + '.' + fid_match.group(2)
                else:
                    print(f"Could not derive fid from FORD field '{ford_field_name}'")
                    continue

                # Read the XLS file
                xls = pd.ExcelFile(file_path)
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet_name)

                    for index, row in df.iterrows():
                        year = int(row['Rok uplatnění'])
                        ut_wos = str(row['UT WoS'])
                        name = truncate_string(str(row['Výsledek']), 8000)
                        type_doc = str(row['Druh dokumentu'])
                        journal_name = str(row['Název časopisu'])
                        issn = str(row['ISSN']) if pd.notna(row['ISSN']) else ''
                        eissn = str(row['E-ISSN']) if pd.notna(row['E-ISSN']) else ''
                        authors = truncate_string(str(row['Autor/ka']), 8000)
                        vo_corresponding_author = truncate_string(str(row['VO korespondenční/ho autora/autorky z ČR']),
                                                                  8000) if pd.notna(
                            row['VO korespondenční/ho autora/autorky z ČR']) else ''
                        international_collaboration = convert_czech_or_slovak(
                            row['Mezinárodní spolupráce'] if pd.notna(row['Mezinárodní spolupráce']) else 'NE')
                        more_than_30_authors = convert_czech_or_slovak(
                            row['30+ autorů/autorek'] if pd.notna(row['30+ autorů/autorek']) else 'NE')
                        author_count = int(row['Celkový počet autorů/autorek']) if pd.notna(
                            row['Celkový počet autorů/autorek']) else 0
                        czech_or_slovak = str(row['Český/slovenský časopis'])
                        vo = truncate_string(str(row['Seznam CZ institucí']), 4000)
                        institution_count = 0
                        ford_num = truncate_string(ford_num, 3) # there is a bug in the input data xslt files, so the ford_num is maximum 3 characters long
                        zone = str(row['Pásmo v ' + ford_num])

                        # Construct the SQL statement for article
                        sql = f"""
                            INSERT INTO {schema}.article (year, UT_WoS, name, type, journal_name, issn, eissn, fid, authors, VO_coresponding_author, author_count, czech_or_slovak, VO, institution_count, zone)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        cursor.execute(sql, year, ut_wos, name, type_doc, journal_name, issn, eissn, fid, authors,
                                       vo_corresponding_author, author_count, czech_or_slovak, vo, institution_count,
                                       zone)

                    conn.commit()
                    print(f"Inserted data from sheet '{sheet_name}' into the database.")


# Helper function to truncate strings to fit within the maximum length
def truncate_string(value, max_length):
    if len(value) > max_length:
        return value[:max_length]
    return value


# Helper function to convert 'NE'/'E' to integers
def convert_czech_or_slovak(value):
    return 1 if value.lower() == 'e' else 0

# Function to strip numeric prefixes and periods from directory names
def strip_prefix(name):
    return re.sub(r'^\d+\.\s*', '', name)


conn = db_connection()
schema = os.getenv('DB_SCHEMA')
directory = '.'
cursor = conn.cursor()

df = pd.read_excel(download_institutions())

create_rvvi_script(conn, cursor)
import_institutions(df, schema, conn, cursor)

download_articles()
import_articles(schema, conn, cursor)

# Close the database connection
cursor.close()
conn.close()

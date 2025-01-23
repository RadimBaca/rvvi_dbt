import os
import re
import pandas as pd
import requests
import zipfile
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from ingest.convert_helpers import strip_prefix, truncate_string, convert_czech_or_slovak

# Pydantic model for journal entries
class JournalEntry(BaseModel):
    year: int
    name: str
    issn: Optional[str]  = None
    eissn: Optional[str]  = None
    article_count: Optional[int]
    zone: str
    czech_or_slovak: str
    fid: int


    @field_validator("issn", "eissn", mode='before')
    def truncate_issn(cls, value):
        if pd.isna(value):  # Check for NaN
            return None
        value = str(value)  # Convert to string in case it's not
        return value[:10] if len(value) > 10 else value

# Pydantic model for article entries
class ArticleEntry(BaseModel):
    year: int
    ut_wos: str
    name: str
    type_doc: str
    journal_name: str
    issn: Optional[str]  = None
    eissn: Optional[str]  = None
    fid: int
    authors: str
    vo_corresponding_author: Optional[str]
    author_count: Optional[int] = None
    czech_or_slovak: str
    vo: Optional[str]
    institution_count: int
    zone: str

    @field_validator("issn", "eissn", mode='before')
    def truncate_issn(cls, value):
        if pd.isna(value):  # Check for NaN
            return None
        value = str(value)  # Convert to string in case it's not
        return value[:10] if len(value) > 10 else value

    @field_validator("author_count", mode="before")
    def handle_nan(cls, value):
        return None if pd.isna(value) else value

# Function to download articles
def download_articles():
    urls = [
        ("https://hodnoceni.rvvi.cz/public/h23/H23%20M2%20biblio%20obory/1.%20Natural%20Sciences.zip", "1. Natural Sciences.zip"),
        ("https://hodnoceni.rvvi.cz/public//h23/H23%20M2%20biblio%20obory/2.%20Engineering%20and%20Technology.zip","2. Engineering and Technology.zip"),
        ("https://hodnoceni.rvvi.cz/public/h23/H23%20M2%20biblio%20obory/3.%20Medical%20and%20Health%20Sciences.zip","3. Medical and Health Sciences.zip"),
        ("https://hodnoceni.rvvi.cz/public/h23/H23%20M2%20biblio%20obory/4.%20Agricultural%20and%20Veterinary%20Sciences.zip","4. Agricultural and Veterinary Sciences.zip"),
        ("https://hodnoceni.rvvi.cz/public/h23/H23%20M2%20biblio%20obory/5.%20Social%20Sciences.zip","5. Social Sciences.zip"),
        ("https://hodnoceni.rvvi.cz/public/h23/H23%20M2%20biblio%20obory/6.%20Humanities%20and%20the%20Arts.zip", "6. Humanities and Arts.zip"),
    ]
    for url, zip_filename in urls:
        try:
            response = requests.get(url, stream=True, verify=False)
            response.raise_for_status()
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

    print("All files downloaded and extracted.")

# Function to construct and execute SQL for journals
def insert_journal(cursor, schema, entry: JournalEntry):
    sql = f"""
        INSERT INTO {schema}.journal (year, name, issn, eissn, article_count, zone, czech_or_slovak, fid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(sql, entry.year, entry.name, entry.issn, entry.eissn, entry.article_count, entry.zone, entry.czech_or_slovak, entry.fid)

# Function to construct and execute SQL for articles
def insert_article(cursor, schema, entry: ArticleEntry):
    sql = f"""
        INSERT INTO {schema}.article (year, UT_WoS, name, type, journal_name, issn, eissn, fid, authors, VO_coresponding_author, 
                                       author_count, czech_or_slovak, VO, institution_count, zone)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(sql, entry.year, entry.ut_wos, entry.name, entry.type_doc, entry.journal_name, entry.issn, entry.eissn,
                   entry.fid, entry.authors, entry.vo_corresponding_author, entry.author_count, entry.czech_or_slovak,
                   entry.vo, entry.institution_count, entry.zone)

# Function to import articles
def import_articles(schema, conn, cursor):
    base_dir = '.'
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if (file.startswith("Priloha_2") or file.startswith("Priloha_3")) and file.endswith('.xlsx'):
                file_path = os.path.join(root, file)

                current_dir = os.path.basename(root)

                if current_dir == "WoS":
                    field_of_study_name = strip_prefix(os.path.basename(os.path.dirname(os.path.dirname(root))))
                    ford_field_name = os.path.basename(os.path.dirname(root))  # Immediate parent directory
                else:
                    field_of_study_name = strip_prefix(os.path.basename(os.path.dirname(root)))
                    ford_field_name = current_dir  # Current directory name

                # Derive fid
                fid_match = re.match(r'^(\d+)\.(\d+)', ford_field_name)
                if not fid_match:
                    print(f"Could not derive fid for {ford_field_name}")
                    continue

                fid = int(fid_match.group(1)) * 10 + int(fid_match.group(2))
                ford_num = truncate_string(fid_match.group(1) + '.' + fid_match.group(2), 3)  # there is a bug in the input data xslt files, so the ford_num is maximum 3 characters long
                cursor.execute(f"SELECT sid FROM {schema}.field_of_study WHERE name = ?", field_of_study_name)
                sid_row = cursor.fetchone()
                if not sid_row:
                    print(f"Field of study '{field_of_study_name}' not found.")
                    continue
                sid = sid_row[0]

                cursor.execute(f"SELECT fid FROM {schema}.field_ford WHERE fid = ?", fid)
                if not cursor.fetchone():
                    cursor.execute(f"INSERT INTO {schema}.field_ford (fid, sid, name) VALUES (?, ?, ?)", fid, sid, ford_field_name)
                    conn.commit()

                print(f"Processing file: {file_path}")
                xls = pd.ExcelFile(file_path)
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    for _, row in df.iterrows():
                        # Construct entries based on file type
                        if file.startswith("Priloha_2"):
                            entry = JournalEntry(
                                year=int(row['Rok uplatnění']),
                                name=row['Název'],
                                issn=row['ISSN'],
                                eissn=row['E-ISSN'],
                                article_count=row.get('Počet dokumentů'),
                                zone=row['Pásmo'],
                                czech_or_slovak=row['Český nebo slovenský časopis'],
                                fid=fid
                            )
                            insert_journal(cursor, schema, entry)
                        elif file.startswith("Priloha_3"):
                            entry = ArticleEntry(
                                year=int(row['Rok uplatnění']),
                                ut_wos=row['UT WoS'],
                                name=truncate_string(row['Výsledek'], 8000),
                                type_doc=row['Druh dokumentu'],
                                journal_name=row['Název časopisu'],
                                issn=row['ISSN'],
                                eissn=row['E-ISSN'],
                                fid=fid,
                                authors=truncate_string(row['Autor/ka'], 8000),
                                vo_corresponding_author=truncate_string(row.get('VO korespondenční/ho autora/autorky z ČR'), 8000),
                                author_count=row.get('Celkový počet autorů/autorek', 0),
                                czech_or_slovak=row['Český/slovenský časopis'],
                                vo=truncate_string(row['Seznam CZ institucí'], 4000),
                                institution_count=0,
                                zone=row[f'Pásmo v {ford_num}']
                            )
                            insert_article(cursor, schema, entry)

                    conn.commit()
                    print(f"Data from sheet '{sheet_name}' inserted.")

import os
import pandas as pd
import pyodbc
import requests
from dotenv import load_dotenv
import zipfile

from article import download_articles, import_articles
from institution import download_institutions, import_institutions


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

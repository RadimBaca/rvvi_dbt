import os, sys
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from ingest.article import download_articles, import_articles
from ingest.institution import download_institutions, import_institutions
from pydantic import BaseModel
from typing import List


class Config(BaseModel):
    db_username: str
    db_password: str
    db_server: str
    db_database: str
    db_schema: str

def parse_arguments(argv: List[str]) -> Config:
    """Parse command-line arguments into a Config model."""
    kwargs = {}
    for arg in argv:
        if arg.startswith("--"):
            key, value = arg[2:].split("=", 1)
            kwargs[key.replace("-", "_")] = value
    return Config(**kwargs)

def db_connection(config: Config) -> pyodbc.Connection:
    connection_string = f'DRIVER={{SQL Server}};SERVER={config.db_server};DATABASE={config.db_database};UID={config.db_username};PWD={config.db_password}'
    # connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    return pyodbc.connect(connection_string)


def create_rvvi_script(conn, cursor, script_path='create_rvvi.sql'):
    """Execute the SQL script for RVVI table creation."""
    try:
        with open(script_path, 'r') as file:
            sql_script = file.read()

        commands = sql_script.split('go\n')

        for command in commands:
            command = command.strip()
            if command:  # Skip empty commands
                cursor.execute(command)
                conn.commit()
        print(f"{script_path} SQL script executed successfully.")
    except Exception as e:
        print(f"Error executing SQL script: {e}")


def main(config: Config):
    """Main function to manage the data processing workflow."""
    try:
        # Establish database connection
        conn = db_connection(config)
        cursor = conn.cursor()

        # Retrieve schema and directory settings
        schema = os.getenv('DB_SCHEMA')

        # Download and process institution data
        institution_data_path = download_institutions()
        df_institutions = pd.read_excel(institution_data_path)

        # Execute RVVI table creation script
        create_rvvi_script(conn, cursor)

        # Import institution data
        import_institutions(df_institutions, schema, conn, cursor)

        # Download and process articles
        download_articles()
        import_articles(schema, conn, cursor)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Ensure resources are properly closed
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


if __name__ == "__main__":
    config = parse_arguments(sys.argv[1:])
    main(config)

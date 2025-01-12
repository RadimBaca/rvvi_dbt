import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from article import download_articles, import_articles
from institution import download_institutions, import_institutions


def db_connection():
    """Establish a connection to the database using environment variables."""
    load_dotenv()
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    if not all([server, database, username, password]):
        raise ValueError("Database connection details are not fully provided in the .env file.")
    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
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


def main():
    """Main function to manage the data processing workflow."""
    try:
        # Establish database connection
        conn = db_connection()
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
    main()

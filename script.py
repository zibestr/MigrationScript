"""
author: Danila Yashin
"""
import os

import sqlalchemy
import pandas as pd
from dotenv import load_dotenv


def make_original_connection_string() -> str:
    """Generate a connection string

    Returns:
        str: string to connect to PostgreSQL database
    """
    load_dotenv()
    user = os.getenv('USER_ORIGINAL')
    password = os.getenv('PASSWORD_ORIGINAL')
    host = os.getenv('HOST_ORIGINAL')
    port = os.getenv('PORT_ORIGINAL')
    db_name = os.getenv('DB_NAME_ORIGINAL')
    return f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'


def make_new_connection_string() -> str:
    """Generate a connection string

    Returns:
        str: string to connect to PostgreSQL database
    """
    load_dotenv()
    user = os.getenv('USER_NEW')
    password = os.getenv('PASSWORD_NEW')
    host = os.getenv('HOST_NEW')
    port = os.getenv('PORT_NEW')
    db_name = os.getenv('DB_NAME_NEW')
    return f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'


def extract_script(filename: str) -> str:
    """Extract SQL script from file

    Args:
        filename (str): script filename

    Returns:
        str: SQL script
    """
    path = os.path.join('scripts', filename)
    with open(path, 'r', encoding='UTF-8') as file:
        script = file.read()
    return script


if __name__ == '__main__':
    db_orig = sqlalchemy.create_engine(make_original_connection_string())
    db_new = sqlalchemy.create_engine(make_new_connection_string())
    for script_filename in os.listdir('scripts'):
        sql = extract_script(script_filename)
        df = pd.read_sql(sql, db_orig)
        df.to_sql(script_filename, db_new, if_exists='replace')

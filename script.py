"""
author: Danila Yashin
"""
import os

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()


def make_original_connection_string() -> str:
    """Generate a connection string

    Returns:
        str: string to connect to PostgreSQL database
    """
    user = os.getenv('USER_ORIGINAL')
    password = os.getenv('PASSWORD_ORIGINAL')
    host = os.getenv('HOST_ORIGINAL')
    port = os.getenv('PORT_ORIGINAL')
    db_name = os.getenv('DB_NAME_ORIGINAL')
    return f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'


def make_new_connection_string() -> str:
    """Generate a connection string

    Returns:
        str: string to connect to MySQL database
    """
    user = os.getenv('USER_NEW')
    password = os.getenv('PASSWORD_NEW')
    host = os.getenv('HOST_NEW')
    port = os.getenv('PORT_NEW')
    db_name = os.getenv('DB_NAME_NEW')
    # return f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'
    return f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}'


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
    print('Initializing original database...')
    db_orig = sqlalchemy.create_engine(make_original_connection_string())
    print('Initializing new database...')
    db_new = sqlalchemy.create_engine(make_new_connection_string())
    print('Load scripts...')
    list_ = tqdm(os.listdir('scripts'))
    for script_filename in list_:
        list_.set_description(script_filename)
        sql = extract_script(script_filename)
        with db_orig.connect() as connection:
            if script_filename.count('.') == 2:
                id_col = script_filename.split('.')[1]
            else:
                id_col = 'id'
            tran = connection.begin()
            df = pd.read_sql(sql, connection,
                             index_col=id_col)
            tran.commit()
        with db_new.connect() as connection:
            tran = connection.begin()
            df.to_sql(script_filename.split('.')[0], db_new,
                      if_exists='replace', index_label=id_col,
                      method='multi')
            tran.commit()
    print('Finish!')

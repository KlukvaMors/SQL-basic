import pandas as pd
from IPython.display import display, Markdown, Latex, Code

import sqlite3
import glob
from pathlib import Path



__all__ = ['sql', 'show', 'table', 'load']

DATA_FOLDER = 'data'

conn = sqlite3.connect(":memory:")

def sql(query, connection=conn):
    '''Просто функция с коротким именем,
    для наглядного представления и исполнения SQL запросов'''
    display(Code(query, language='SQL'))
    connection.execute(query)

 
def show(query, connection=conn):
    '''Просто функция с коротким именем,
    для наглядного представления SELECT запросов
    в виде pandas dataframe
    '''
    return pd.read_sql_query(query, connection)


def table(table_name:str, connection=conn):
    """Показывает содержимое таблицы 
    """
    return show(f'SELECT * FROM {table_name}', connection)


def schema(name, connection=conn):
    c = connection.cursor()
    c.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{name}'")
    sql_schema = c.fetchall()[0][0]
    display(Code(sql_schema, language="SQL"))


def load_sql(file_name:str, connection=conn):
    path = Path(file_name)
    if not path.exists():
        raise Exception("Указаный путь не существует")
    if not path.is_file():
        raise Exception("Указаный путь не указывает на файл")
    with path.open() as file:
        sql_schema = file.read()
        connection.execute(sql_schema)
        display(Code(sql_schema, language="SQL"))

def load_csv(file_name, connection=conn):
    path = Path(file_name)
    if not path.exists():
        raise Exception("Указаный путь не существует")
    if not path.is_file():
        raise Exception("Указаный путь не указывает на файл")
    df = pd.read_csv(file_name)
    df.to_sql(path.stem, connection, if_exists="append", index=False)
    return df
    

def load(folder: str, connection=conn):
    for data_csv in glob.glob(f'{DATA_FOLDER}/{folder}/*.csv'):
        load_csv(data_csv, connection)
    
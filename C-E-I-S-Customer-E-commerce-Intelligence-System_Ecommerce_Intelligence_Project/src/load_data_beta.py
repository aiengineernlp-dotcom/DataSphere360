import pandas as pd
from sqlalchemy import create_engine
import time
import os
import re

current_time = time.time()
pd.set_option('display.expand_frame_repr', False)

engine_erman_connexion_to__dataspere360 = create_engine(
    'postgresql://postgres:postgres@localhost:5555/datasphere360_customer_ecommerce')


def push_data_to_psql(filepath: str, table_name: str) -> str:
    """
    :param filepath:  the path of my csv file
    :param table_name: the name of my table
    :return: just a confirmation message to ensure that my files are on the database Psql
    :errors : Exception, ValueError,FileNotFoundError
    """

    if not filepath:
        raise FileNotFoundError("The file doest not exist")
    else:
        try:
            df = pd.read_csv(filepath)
            table_name = os.path.splitext(os.path.basename(filepath))[0]
            df.to_sql(table_name, con=engine_erman_connexion_to__dataspere360, if_exists='replace', index=False)
            what_is_up = (f' ✅ All is GOOD Bro ! i make it... the table {table_name} is on sql {current_time}')
        except Exception as e:
            what_is_up = (f' ❌ All is BAB Bro ! i did not make it... the table {table_name} is not on sql, {current_time}')
            raise ValueError(f" ❌ Sorry Bro something when wrong during the creation of the table {table_name} the error may be : {e} {current_time}")
    return what_is_up

customers = push_data_to_psql('../E_commerce_datasets/customers.csv', "customers")
location = push_data_to_psql('../E_commerce_datasets/customers.csv', 'location')
order_item = push_data_to_psql('../E_commerce_datasets/customers.csv', 'order_item')
orders = push_data_to_psql('../E_commerce_datasets/customers.csv', "orders")
products = push_data_to_psql('../E_commerce_datasets/customers.csv', "products")
reviews = push_data_to_psql('../E_commerce_datasets/customers.csv', "reviews")
sellers = push_data_to_psql('../E_commerce_datasets/customers.csv', "sellers")
category_translation = push_data_to_psql('../E_commerce_datasets/customers.csv',
                                         'category_translation')
payments = push_data_to_psql('../E_commerce_datasets/customers.csv', "payments")


# ------------------------------

def fetch_data_from_psql(
        engine_erman_connexion_to___) -> dict:
    """
    USE CASE: this fuction is for fetching data from the database sql. He can be use with other database.
    Returns :
        - Dict: a dictionnary with my data inside

    """
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
    tables = pd.read_sql(query, con=engine_erman_connexion_to___)[
        'table_name'].tolist()
    all_data_fetch_from_sql = {}
    for table in tables:
        print(f"Recuparation of the table :{table}  -> in :{round((current_time))}s")
        all_data_fetch_from_sql[table] = pd.read_sql(f'SELECT * FROM "{table}"', con=engine_erman_connexion_to___)
    return all_data_fetch_from_sql


fetch_dataSet = fetch_data_from_psql(engine_erman_connexion_to__dataspere360)
print(fetch_dataSet['customers'].describe())

print('\n')
print('\n')


def inspect_data_structure_in_360(data_from_sql: dict) -> pd.DataFrame:
    ''''
    Use case: This fuction is for inspecting data structure in 360
    it can be use to retrieve a specific data structure but in that case one additional param like 'data_table_name:str'
    need to be add to inspect_data_structure_in_360. OTHERWISE, use the curent fonction is fol all data table at once.

    :arg:
        - data_from_sql : dict
        - data_table : str
    :returns :
        - pd.DataFrame
    :errors:
        - ValueError
    '''
    for data_table in data_from_sql:
        df = data_from_sql[
            data_table]
        print(f"{'█' * 70} ANALYSE TABLE {data_table} {'█' * 55}")
        print(df.head())
        print(df.describe())
        print(df.info())
    return df.head(), df.info(), df.describe()  # can use


my_sql_dataset = fetch_dataSet
head, info, describe = inspect_data_structure_in_360(my_sql_dataset)

# DROP TABLE SECUTITY
from sqlalchemy import text

with engine_erman_connexion_to__dataspere360.connect() as conn:
    try:
        conn.execute(text(
            'DROP TABLE "../python_project_aiml_logicmojo_dataset/customers.csv" '))
        conn.commit()
        print(fr' ✅  Table droped')
    except Exception as e:
        print(fr"❌ Error Bro  look at {e}")

print('\n')
print('\n')


def identify_fk_pk(data_from_sql: dict) -> dict:
    all_data = {}
    potential = []
    look_keys_pattern = re.compile(r'.*(id|pk|code|fk|pk).*', re.IGNORECASE)
    for data_table in data_from_sql:
        df = data_from_sql[data_table]
        all_data[data_table] = df

    for data_table, df in all_data.items():
        potential_cols = [col for col in df.columns if
                          look_keys_pattern.match(col)]

        for col in potential_cols:

            is_unique = df[col].nunique() == len(df)

            tipo = "PK (Primary Key)" if is_unique else "FK (Foreing Key)"
            print(f"Table [{data_table}] -> Key DEtected at : {col} ({tipo})\n {'-' * 50} ")

    return potential_cols


r = identify_fk_pk(fetch_dataSet)
print(r)






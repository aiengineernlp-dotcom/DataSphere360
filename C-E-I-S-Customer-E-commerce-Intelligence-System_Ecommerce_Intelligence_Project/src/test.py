import pandas as pd
from sqlalchemy import create_engine
import time
import os

current_time = time.time()
# # 1. Empêche le retour à la ligne automatique
pd.set_option('display.expand_frame_repr', False)
# 2. Affiche toutes les colonnes (au cas où il y en aurait beaucoup)
# pd.set_option('display.max_columns', None)


# -0- connection to the data base PostgreSql that i choose to use
engine_erman_connexion_to__dataspere360 = create_engine(
    'postgresql://postgres:postgres@localhost:5555/datasphere360_customer_ecommerce')


# -1- I need to send my data to the Database PostgreSql,
def push_data_to_psql(filepath: str, table_name: str) -> str:
    """
    :param filepath:  the path of my csv file
    :param table_name: the name of my table
    :return: just a confirmation message to ensure that my files are on the database Psql
    :errors : Exception, ValueError,FileNotFoundError
    """

    # this condition is for directly make sure that the file exist otherwise we do not continous
    if not filepath:
        raise FileNotFoundError("The file doest not exist")
        # if the file exist, then i read it it like a pandas because later i will need to use a pandas file to convert into sql
    else:
        try:
            df = pd.read_csv(filepath)
            table_name = os.path.splitext(os.path.basename(filepath))[0]
            '''
            because if i just give 'filepath' to_sql, Postgresql will create a file table name like "../python_project_aiml_logicmojo_dataset/customers.csv', "customers". to avoir it i use os.path.basename
            it will remove all thing just before my real file name. before basename: ../dossier/data/customers.csv | after basename":customers.csv
            now the extention ".csv" -> os.path.splitext will split customers.csv in tuple/list -> ('customers', 'csv'). And finaly,this [0] will collect the word ->customers

            '''

            df.to_sql(filepath, con=engine_erman_connexion_to__dataspere360, if_exists='replace', index=False)
            bar = '▇'  # will come back for this guy later on
            what_is_up = (f' ✅ All is GOOD Bro ! i make it... the table {table_name} is on sql {current_time}')
        except Exception as e:
            what_is_up = (
                f' ❌ All is BAB Bro ! i did not make it... the table {table_name} is not on sql, {current_time}')
            raise ValueError(
                f" ❌ Sorry Bro something when wrong during the creation of the table {table_name} the error may be : {e} {current_time}")
    return what_is_up


# Utilisation


# customers = push_data_to_psql('../python_project_aiml_logicmojo_dataset/customers.csv', "customers")
# category_translation = push_data_to_psql('../python_project_aiml_logicmojo_dataset/customers.csv',
#                                          'category_translation')
# location = push_data_to_psql('../python_project_aiml_logicmojo_dataset/customers.csv', 'location')
# order_item = push_data_to_psql('../python_project_aiml_logicmojo_dataset/customers.csv', 'order_item')
# orders = push_data_to_psql('../python_project_aiml_logicmojo_dataset/customers.csv', "orders")
# products = push_data_to_psql('../python_project_aiml_logicmojo_dataset/customers.csv', "products")
# reviews = push_data_to_psql('../python_project_aiml_logicmojo_dataset/customers.csv', "reviews")
# sellers = push_data_to_psql('../python_project_aiml_logicmojo_dataset/customers.csv', "sellers")

# print(customers)


# category_translation = push_data_to_psql('../python_project_aiml_logicmojo_dataset/customers.csv','category_translation')
# print(category_translation)
# payments = push_data_to_psql('../python_project_aiml_logicmojo_dataset/customers.csv', "payments")
# print(payments)

# ------------------------------


def fetch_data_from_psql(
        engine_erman_connexion_to___) -> dict:  # I'm using this long name just beacause i want to personalize .
    """
    USE CASE: this fuction is for fetching data from the database sql. He can be use with other database.
    Returns :
        - Dict: a dictionnary with my data inside

    """
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "  # this is like a prompt who follows the path location of my data in postgreSQL. Im just saying that i want to SELECT all table table.schema.
    tables = pd.read_sql(query, con=engine_erman_connexion_to___)[
        'table_name'].tolist()  # After selection, I put them in the list, so i can loop in front and back
    all_data_fetch_from_sql = {}
    for table in tables:
        print(f"Recuparation of the table :{table}  -> in :{round((current_time))}s")
        # I load each table in the dictionnary with is name like a key and his value is the query sql
        all_data_fetch_from_sql[table] = pd.read_sql(f'SELECT * FROM "{table}"', con=engine_erman_connexion_to___)
        # print(all_data_fetch)
    return all_data_fetch_from_sql


fetch_dataSet = fetch_data_from_psql(engine_erman_connexion_to__dataspere360)
print(fetch_dataSet['customers'].describe())

# df_customers = pd.read_sql('SELECT * from customers limit 10',con=engine)
# print(df_customers.info())


#
# def inspect_data_structure_in_360(data: dict) -> pd.DataFrame:
#     ''''
#     Use case: This fuction is for inspecting data structure in 360
#     it can be use to retrieve a specific data structure but in that case one additional param like 'data_table_name:str'
#     need to be add to inspect_data_structure_in_360. OTHERWISE, use the curent fonction is fol all data table at once.

#     :arg:
#         - data : dict
#         - data_table_name : str
#     :returns :
#         - pd.DataFrame
#     :errors:
#         - ValueError
#     '''
#     for csv_file in data:
#         df = data[csv_file]  # i collect the key of my dictiannary who will come from fetch_data_from_psql function
#         print(f"{'█' * 70} ANALYSE TABLE {csv_file} {'█' * 55}")
#         print(df.head())
#         print(df.describe())
#         print(df.info())
#     return df.head(), df.info(),df.describe()   # can use

# my_sql_dataset = fetch_dataSet  # la capture du dictionnaire all_data_fetch_from_sql qui est ejectee dans l'espace se fait via l'appel de sa fonction
# head, info, describe = inspect_data_structure_in_360(my_sql_dataset)

# print(f'{head}')


# DROP TABLE SECUTITY
from sqlalchemy import text

with engine_erman_connexion_to__dataspere360.connect() as conn:
    try:
        conn.execute(text(
            'DROP TABLE "../python_project_aiml_logicmojo_dataset/customers.csv" '))  # this explain more more the use of:  table_name = os.path.splitext(os.path.basename(filepath))[0]
        conn.commit()
        print(fr' ✅  Table droped')
    except Exception as e:
        print(fr"❌ Error Bro  look at {e}")


def identify_keys(data_from_sql: dict) -> pd.DataFrame:
    """
    this fucntion will identify primary primary and forein keys in the data .

    Args:
        - data: dictionnary
    Returns:
        - pd.DataFrame

    Errors:
        - ValueError
        -
    """
    all_columns = {}
    for data_table in data_from_sql:
        df = data_from_sql[data_table]     # Aligne le return exactement ici (zéro espace de décalage par rapport au for)
        all_columns[data_table] = df.columns
    return all_columns


r = identify_keys(fetch_dataSet)

print(f"|*************************> {type(r)}<*************************|")
print('\n')
print(f"{r} ")

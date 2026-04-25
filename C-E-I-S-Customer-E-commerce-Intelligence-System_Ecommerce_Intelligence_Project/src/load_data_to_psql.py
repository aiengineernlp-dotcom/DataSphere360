import pandas as pd
from sqlalchemy import create_engine
import time
import os
import re

current_time = time.time()
# # 1. stop the automatic back to the ligne when display datasets
pd.set_option('display.expand_frame_repr', False)
# 2. display all columns
# pd.set_option('display.max_columns', None)


# -0- connection to the data base PostgreSql that i choose to use
engine_erman_connexion_to__dataspere360 = create_engine('postgresql://postgres:postgres@localhost:5555/datasphere360_customer_ecommerce')


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

            df.to_sql(table_name, con=engine_erman_connexion_to__dataspere360, if_exists='replace', index=False)
            bar = '▇'  # will come back for this guy later on
            what_is_up = (f' ✅ All is GOOD Bro ! i make it... the table {table_name} is on sql {current_time}')
        except Exception as e:
            what_is_up = (
                f' ❌ All is BAB Bro ! i did not make it... the table {table_name} is not on sql, {current_time}')
            raise ValueError(
                f" ❌ Sorry Bro something when wrong during the creation of the table {table_name} the error may be : {e} {current_time}")
    return what_is_up

# Utilisation

customers = push_data_to_psql('../E_commerce_datasets/customers.csv', "customers")
location = push_data_to_psql('../E_commerce_datasets/customers.csv', 'location')
order_item = push_data_to_psql('../E_commerce_datasets/customers.csv', 'order_item')
orders = push_data_to_psql('../E_commerce_datasets/customers.csv', "orders")
products = push_data_to_psql('../E_commerce_datasets/customers.csv', "products")
reviews = push_data_to_psql('../E_commerce_datasets/customers.csv', "reviews")
sellers = push_data_to_psql('../E_commerce_datasets/customers.csv', "sellers")
category_translation = push_data_to_psql('../E_commerce_datasets/customers.csv',                                      'category_translation')
payments = push_data_to_psql('../E_commerce_datasets/customers.csv', "payments")

print(customers)
print(location)
print(order_item)
print(orders)
print(products)
print(reviews)
print(sellers)
print(category_translation)
print(payments)


##------------------------------



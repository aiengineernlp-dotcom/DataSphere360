import pandas as pd
from sqlalchemy import create_engine
# 1 - connection to database PosgreSQL
# Format : 'postgresql://utilisateur:motdepasse@localhost:5432/nom_de_ta_base'
engine = create_engine('postgresql://postgres:postgres@localhost:5555/datasphere360_customer_ecommerce')
def push_data_file_to_sql(filepath, table_name) -> str:
    if filepath is not None:
        try:
            df = pd.read_csv(filepath)
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            what_is_up = (f" ✅ Well Done Table {table_name} created dans sql successfully ! ")
        except Exception as e:
            raise ValueError(f" Look at: {e}")
    else:
        raise FileNotFoundError (f" ❌ Error:  Failed to create the table {table_name} dans sql ")
    return what_is_up


customers = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "customers")
print(customers)
category_translation = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv','category_translation')
print(category_translation)
payments = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "payments")
print(payments)

# # 1. Empêche le retour à la ligne automatique
# pd.set_option('display.expand_frame_repr', False)
# 2. Affiche toutes les colonnes (au cas où il y en aurait beaucoup)
pd.set_option('display.max_columns', None)
#
# customers = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "customers")
# category_translation = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv','category_translation')
# location = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', 'location')
# order_item = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', 'order_item')
# orders = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "orders")
# products = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "products")
# reviews = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "reviews")
# sellers = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "sellers")
# print(customers['category_translation'].describe)
#


# df_customers = pd.read_sql('SELECT * FROM customers', con=engine)


# print(category_translation.head())
# print(location.head())
# print(order_item.head())
# print(orders.head())
# print(payments.head())
# print(products.head())
# print(reviews.head())
# print(sellers.head())
#
#
def fetch_data_from_sql():
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"

    tables = pd.read_sql(query, con=engine)['table_name'].tolist()  # engine pour te connecter avec cette requette (query) et aller directement sur les tables et les listés
    print(tables)
    all_data = {}

    for table in tables:
        print(f"Recuperation de la table: {table}")
        # all_data[table] = pd.read_sql(f'SELECT * FROM {table}',con=engine) #  all_data[table] est la cle et l'autre membre la valeur
        all_data[table]  = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",con=engine)
    return all_data # je vais le capturer en bas dans  my_sql_dataset -  all_data  # vu que le linge sale se lave en famille, pour utiliser ces all_data dehors, je dois le recuperer en creant pour lui une variable.  #Quand tu fais return all_data, tu "éjectes" le contenu hors de la fonction, mais tu dois l'attraper dans une variable à l'extérieur pour l'utiliser ailleurs.



# --- UTILISATION ---
# my_sql_dataset = fetch_data_from_sql() # merci_ici : la capture

#
#
#
def inspect_data_structure(data: dict) -> pd.DataFrame:
    for csv_file in data:
        df = data[csv_file]  # merci_ici
        head = df.head()
        info = df.info()
        describe = df.describe()
    return head, info, describe

# capturer_dict = all_dataframes  # merci_ici : la capture
my_sql_dataset = fetch_data_from_sql() # merci_ici : la capture
head, info, describe = inspect_data_structure(my_sql_dataset)

print(f'{head}\n')

# I will maybe use this methode because its is more efficient, also my data are comming directly from de databse so my Ram is more faster. how ever let see the the load_data_version_3 (upload all folder and loop in)

#
#
#
#

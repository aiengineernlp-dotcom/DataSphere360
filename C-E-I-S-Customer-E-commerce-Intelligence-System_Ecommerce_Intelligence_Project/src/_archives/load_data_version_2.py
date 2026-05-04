import pandas as pd
from sqlalchemy import create_engine

# 1 - connection to database PosgreSQL
# Format : 'postgresql://utilisateur:motdepasse@localhost:5432/nom_de_ta_base'
engine = create_engine('postgresql://postgres:postgres@localhost:5555/datasphere360_customer_ecommerce')

def push_data_file_to_sql(filepath, table_name)->str:
    if filepath is not None:
        try:
            df = pd.read_csv(filepath)
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            what_is_up= (f" ✅ Well Done Table {table_name} created successfully ! ")
        except Exception as e:
            raise ValueError(f" Look at: {e}")
    else:
        raise FileNotFoundError (f" ❌ Error:  Failed to create the table {table_name} ")
    return what_is_up

customers = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "customers")
print(customers)
category_translation = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', 'category_translation')
# location = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', 'location')
# order_item = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', 'order_item')
# orders = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "orders")
# payments = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "payments")
# products = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "products")
# reviews = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "reviews")
# sellers = push_data_file_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "sellers")


df_customers = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", con=engine)
print(df_customers)




# print(category_translation.head())
# print(location.head())
# print(order_item.head())
# print(orders.head())
# print(payments.head())
# print(products.head())
# print(reviews.head())
# print(sellers.head())






# I will maybe use this methode because its is more efficient, also my data are comming directly from de databse so my Ram is more faster. how ever let see the the load_data_version_3 (upload all folder and loop in)

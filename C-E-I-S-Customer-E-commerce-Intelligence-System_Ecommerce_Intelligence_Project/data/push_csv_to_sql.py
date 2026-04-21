import pandas as pd
from sqlalchemy import create_engine

# 1- connection to PostgreSQL
# Format : 'postgresql://utilisateur:motdepasse@localhost:5432/nom_de_ta_base'
engine = create_engine('postgresql://postgres:postgres@localhost:5555/datasphere360_customer_ecommerce')
# 2 - charger et envoyer vers SQL
def csv_to_sql(csv_filepath, table_name ):
    df = pd.read_csv(csv_filepath)
    try:
        # to_sql cree la table rapidement avec les bonnes colonnes
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        done = (f"table {table_name} created successfully ! ")
    except Exception as e:
        done = (f"table {table_name} failed ! ")

    return done

customers = csv_to_sql('../python_project_aiml_logicmojo_dataset/customers.csv', "customers")
orders = csv_to_sql('../python_project_aiml_logicmojo_dataset/orders.csv', "orders")
# order_item = csv_to_sql('../python_project_aiml_logicmojo_dataset/order_item.csv',"order_item")
# payments = csv_to_sql('../python_project_aiml_logicmojo_dataset/payments.csv',"payments")
# reviews = csv_to_sql('../python_project_aiml_logicmojo_dataset/reviews.csv',"reviews")
# products = csv_to_sql('../python_project_aiml_logicmojo_dataset/products.csv',"products")
# sellers = csv_to_sql('../python_project_aiml_logicmojo_dataset/sellers.csv',"sellers")
# location = csv_to_sql('../python_project_aiml_logicmojo_dataset/location.csv',"location")
# category_translation = csv_to_sql('../python_project_aiml_logicmojo_dataset/category_translation.csv',"category_translation")

print(customers)
print(orders)
# print(order_item)
# print(payments)
# print(reviews)
# print(products)
# print(sellers)
# print(location)
# print(category_translation)

print("✅ Data load to sql successully")

df_customers  = pd.read_sql('SELECT * FROM customers LIMIT 10',engine)
print(df_customers.info())





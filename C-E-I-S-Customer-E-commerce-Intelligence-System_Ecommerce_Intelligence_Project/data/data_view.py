import pandas as pd

customers = pd.read_csv('../python_project_aiml_logicmojo_dataset/customers.csv')
orders = pd.read_csv('../python_project_aiml_logicmojo_dataset/orders.csv')
order_item = pd.read_csv('../python_project_aiml_logicmojo_dataset/order_item.csv')
payments = pd.read_csv('../python_project_aiml_logicmojo_dataset/payments.csv')
reviews = pd.read_csv('../python_project_aiml_logicmojo_dataset/reviews.csv')
products = pd.read_csv('../python_project_aiml_logicmojo_dataset/products.csv')
sellers = pd.read_csv('../python_project_aiml_logicmojo_dataset/sellers.csv')
location = pd.read_csv('../python_project_aiml_logicmojo_dataset/location.csv')
category_translation = pd.read_csv('../python_project_aiml_logicmojo_dataset/category_translation.csv')


print(category_translation.head())
print(customers.head())
print(location.head())
print(order_item.head())
print(orders.head())
print(payments.head())
print(products.head())
print(reviews.head())
print(sellers.head())


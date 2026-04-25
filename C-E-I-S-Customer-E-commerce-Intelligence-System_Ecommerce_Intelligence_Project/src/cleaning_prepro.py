import pandas as pd
from sqlalchemy import create_engine

# -0- connection to the data base PostgreSql
engine_erman_connexion_to__dataspere360 = create_engine('postgresql://postgres:postgres@localhost:5555/datasphere360_customer_ecommerce')




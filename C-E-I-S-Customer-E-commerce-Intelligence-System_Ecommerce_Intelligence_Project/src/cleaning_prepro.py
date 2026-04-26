import pandas as pd
from sqlalchemy import create_engine

import pandas as pd
from sqlalchemy import create_engine

# -0- connection to the data base PostgreSql
from sqlalchemy import create_engine
import pandas as pd

engine_erman_connexion_to__dataspere360 = create_engine(
    'postgresql://postgres:postgres@localhost:5555/datasphere360_customer_ecommerce')


def fecth_data_from_sql(engine_erman_connexion_to__) -> dict:
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "  # my query: I need to know what i want to collect, in this case the table names
    tables = pd.read_sql(query, con=engine_erman_connexion_to__)[
        "table_name"].tolist()  # I put them look like a list of tables
    all_data_fetch_from_sql = {}

    for table in tables:
        all_data_fetch_from_sql[table] = pd.read_sql(f'SELECT * FROM "{table}"', con=engine_erman_connexion_to__)
    return all_data_fetch_from_sql


data_fecht_from_sql = fecth_data_from_sql(engine_erman_connexion_to__dataspere360)


def handle_missing_values(data_from_sql: dict) -> pd.DataFrame:
    treshold = 0.3
    all_missing = []
    missing_values_great_30 = {}
    for data_table in data_from_sql:
        df = data_from_sql[data_table]

        for col_name in df.columns:
            col_value = df[col_name]
            # operation extraction direct. I will missing value with no pity

            # -1- null values
            is_missing = col_value.isnull().sum()
            if is_missing > 0:
                all_missing.append(is_missing)
            # print(is_missing)

            # -2- more than 30% missing
            more_than_30 = is_missing > treshold * len(df)
            if more_than_30:
                print(
                    f'This value empty at more than 30% from de df.columns {col_name}')  # if the  rate of empty value is greater than 30%, it should be removed.
                missing_values_great_30[col_name] = is_missing
            else:
                print("Taille est", len(df))
                print(f' moins de vide pour {col_name}')

            for col_name in df.columns:
                if is_missing > 0 and is_missing > treshold * len(df):
                    df_clean_1 = df.drop(col_name)

                    print(df_clean_1)
    # numerical  variable could be manage using median but,  At this point there is only variable that the rate vs len(df.columns) is >30%
    #  categorial variable could be manage using mode but,  At this point there is only variable that the rate vs len(df.columns) is >30%
    return  # we will just remove the value that the rate is >30%


r = handle_missing_values(data_fecht_from_sql)

print(r)
print(type(r))

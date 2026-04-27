
from sqlalchemy import create_engine
import pandas as pd

# -0- connection to the data base PostgreSql
engine_erman_connexion_to__dataspere360 = create_engine(
    'postgresql://postgres:postgres@localhost:5555/datasphere360_customer_ecommerce')


def fecth_data_from_sql(engine_erman_connexion_to__) -> dict:
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "  # my query: I need to know what i want to collect, in this case the table names
    tables = pd.read_sql(query, con=engine_erman_connexion_to__)[
        "table_name"].tolist()  # I put them look like a list of tables
    all_data_fetch_from_sql = {}

    for table in tables:
        all_data_fetch_from_sql[table] = pd.read_sql(f'SELECT * FROM "{table}"', con=engine_erman_connexion_to__)
        print(f'{table} fetched' )
    return all_data_fetch_from_sql

data_fecht_from_sql = fecth_data_from_sql(engine_erman_connexion_to__dataspere360)

print(f"{'='*40}fecth_ data_ from_sql  {'='*40}")
print(f" {data_fecht_from_sql} ")



def handle_missing_values(data_from_sql: dict) -> dict:
    treshold = 0.3
    all_missing = []
    missing_values_great_30 = {}
    for table_name, df in data_from_sql.items():
        # print(df.info())
        for col_name in df.columns:
            col_value = df[col_name]  # access to the columns df. So col_value here is the value column at each loop
            # operation extraction direct.

            # -1- null values
            is_missing = col_value.isnull().sum()

            if is_missing > 0:  # just for a small log verification . I dont really need this condition
                all_missing.append(is_missing)
            # print((is_missing))

            # -2- more than 30% missing values
            more_than_30 = is_missing > (treshold * len(df))

            if more_than_30:
                # print (f'This value empty at {col_name} is\n🚨MORE🚨 than 30% from de len(df) total') # if the  rate of empty value is greater than 30%, it should be removed.
                missing_values_great_30[col_name] = is_missing
            else:
                # print (f' This value empty at {col_name} is\n✅LESS✅ than 30% from de len(df) total')
                if is_missing > 0:
                    # -3- base on type of variable, I will transform missing values to :
                    # Median for numerical  variables,
                    if pd.api.types.is_numeric_dtype(col_value):
                        # compute the median
                        median_col_value = col_value.median()
                        # Replace by the median
                        df[col_name] = col_value.fillna(median_col_value)
                        print(f"Numerical:{col_name} - Median is : {median_col_value}")

                    # Mode for categorial variables
                    elif pd.api.types.is_object_dtype(col_value) or pd.api.types.is_categorical_dtype(col_value):
                        # compute the mode
                        mode_col_value = col_value.mode()[
                            0]  # because .mode() always return a list. and i can not put a list in a dataframe
                        # Replace by the mode (la valeur la plus frequente)
                        df[col_name] = col_value.fillna(mode_col_value)
                        print(f"categorial:{col_name} - mode is: {mode_col_value}\n")
                else:
                    print(f"🟢 {col_name} : all is fine") # i need some others data to validate this output

    return data_from_sql  # dictionnairy of dataframe clean


imputation = handle_missing_values(data_fecht_from_sql)

print(imputation)
print(type(imputation))


def remove_duplicated_record(data_clean_from_sql: dict) -> dict:
    print("\n🚀 ÉTAPE 2 : Suppression des doublons...")

    duplicated_records = []

    for table_name, df_clean in data_clean_from_sql.items():
        if df_clean.duplicated().any():
            duplicated_records.append(df_clean[df_clean.duplicated()])

        df_clean.drop_duplicates(inplace=True)

    return duplicated_records


r = remove_duplicated_record(imputation)

print(fr"{'=' * 40}remove_duplicated_record  {'=' * 40}")
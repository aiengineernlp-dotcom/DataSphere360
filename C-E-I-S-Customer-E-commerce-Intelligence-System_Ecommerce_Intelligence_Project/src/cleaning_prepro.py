
from sqlalchemy import create_engine
import pandas as pd
import datetime

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

    return data_clean_from_sql


clean_data = remove_duplicated_record(imputation)

print(fr"{'=' * 40}remove_duplicated_record  {'=' * 40}")


def convert_date_col_to_date_time_format(data_sql_clean: dict) -> dict:
    print("\n🚀 ETAPE  Convert date columns to datetime format ...")
    liste = []
    for table_name, df in data_sql_clean.items():  # level 1 : only at the overview of table_name and df
        for col_name in df.columns:  # level 2 inside df and table_name
            # print(df[col_name]) # log verification

            if pd.api.types.is_datetime64_any_dtype(df[col_name]) or pd.api.types.is_datetime64_dtype(df[col_name]):
                print(f" {col_name} already converted ")
                continue

            if 'date' in col_name.lower():
                liste.append(col_name)
                df[col_name] = pd.to_datetime(df[col_name])  # ignore: if can't be conerv=ted it will leave it as its is
                print(f"✅ Convertion of {col_name} Done with the  else block")
    return data_sql_clean # data_sql_clean.keys() can be use to see the keys but not recommended

convertion_to_datetime = convert_date_col_to_date_time_format(clean_data)
print(convertion_to_datetime)


def validate_data_type_and_range(data_clean: dict, max_value: int, min_val: int, expected_type) -> dict:
    """
    Use case :
        - Type validation (Verification) : Base on logic of how the type should be, we need to know if the the type expected is actually what we have. Exemple: df['age'] should ne type int64, not something else,
        - Range validation (Verification) : Base on logic of how the range should be, we need to know if we are in the normal range. Exemple df['age'] should not be up to 130.
        -

    Args:
        - data_clean :
        - max_value: is the value that fit my need, the value that df[col_name] SHOULD NOT CROSS UP
        - min_val : is the value that fit my need, the value that df[col_name] SHOULD NOT CROSS DOWN

    Returns:
        - dict:  report result
    """
    final_report = {}
    for data_table, df in data_clean.items():
        for col_name in df.columns:
            results = {"Columns": col_name, "errors": []}
            col_value = df[col_name]

            # validation of type (Verification if the type that my col_name is same as expected)
            if not pd.api.types.is_dtype_equal(df[col_name].dtype, expected_type):  # if type are not egual
                results['errors'].append(
                    f"Type Error bro: Expected type is {expected_type} But receive is {df[col_name].dtype}")

            # validation of type (Verification if the range that my col_name has is  "normal or logic ")
            if pd.api.types.is_numeric_dtype(df[col_name]):
                if min_val is not None and (df[col_name].min() < min_val):
                    results['errors'].append(f"Min value of {col_name} out of range {df[col_name].min()}  < {min_val}")

                if max_value is not None and (df[col_name].max() > max_value):
                    results['errors'].append(
                        f"Max value of {col_name} out of range {df[col_name].max()}  > {max_value}")

            if results["errors"]:
                final_report[f"{data_table}.{col_name}"] = results['errors']
                print(f"❌ Error on the results {results['errors']}, for the columns {results['Columns']}  ")
            else:
                print(f"{col_name} ✅  YOU are good to go data validated!!")
    return final_report


r = validate_data_type_and_range(convertion_to_datetime, 100000, 0, object)
print(r)


def standardize_col_name(data_clean: dict)->dict:

    for table_name, df in data_clean.items():
        df.columns = (df.columns.str.strip().str.lower().str.replace(' ', '_',regex=False).str.replace('.','_',regex=False))
        print(f"Column standartized for the columns:  {table_name}")

    return data_clean
r = standardize_col_name(convertion_to_datetime)


# version -2-
# def standardize_col_name(data_clean: dict) -> dict:
#     for table_name, df in data_clean.items():
#         old_columns = list(df.columns[:3])
#         df.columns = (
#             df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False).str.replace('.', '_', regex=False))
#         new_columns = list(df.columns[:3])
#         print(f"Column standartized for the columns:  {table_name}")
#
#     return data_clean
#
#
# r = standardize_col_name(convertion_to_datetime)
# print(r)
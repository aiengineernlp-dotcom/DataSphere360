from sqlalchemy import create_engine
import pandas as pd
from initial_exploration import f_identify_fk_pk
from cleaning_prepro import data_clean_final

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




def data_integration(data_clean_from_sql: dict) -> pd.DataFrame:
    """
    """
    keys = f_identify_fk_pk(data_clean_from_sql)  # i call the function that identify keys

    main_df = data_clean_from_sql[
        "orders"].copy()  # i create a copy a pivot base on data and i make a copy to secure the data source
    tables_fusionnees = []

    for key_full_name, tipo in keys.items():
        table_name, col_name = key_full_name.split('.')

        if table_name != "orders" and table_name not in tables_fusionnees:
            if col_name in main_df.columns:
                print(f"✅ Fusion unique : orders + {table_name} sur la colonne {col_name}")

                df_to_add = data_clean_from_sql[table_name]
                cols_to_keep = [col for col in df_to_add.columns if col not in main_df.columns or col == col_name]

                main_df = pd.merge(
                    main_df,
                    df_to_add[cols_to_keep],  #
                    on=col_name,
                    how='left'
                )

                tables_fusionnees.append(table_name)

    return main_df


r_data_integration = data_integration(data_clean_final) # I use the result of the function standardize_col_name
print(f"\nSUCCESS ! Nombre de colonnes : {len(r_data_integration.columns)}")
# print(r_data_integration.columns)

print(data_clean_final['order_item'].head())
print(r_data_integration.isnull().sum().sort_values(
    ascending=False))  # Last verification to see if null values has been created after the fusion




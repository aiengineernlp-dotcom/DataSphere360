import pandas as pd
def data_loading_with_pandas_each_files(filepath: str) -> pd.DataFrame:
    mon_dic = {}  # I will see what to do with this guy later
    if filepath is not None:

        try:
            df = pd.read_csv(filepath)
            done = (f" {df} \n {'Success !'}")
        except Exception as e:
            raise NameError(f" An Name Error occure {e}")

    else:
        done = (f" Something when Wrong  {'Failed !'}")

    return done


result1 = data_loading_with_pandas_each_files('../python_project_aiml_logicmojo_dataset/customers.csv')
result2 = data_loading_with_pandas_each_files('../python_project_aiml_logicmojo_dataset/location.csv')
# result4 =
# result5 =
# result6 =
# result7 =
# result8 =
# result9 =
# ....

print(result1)
print(result2)

# I will not continue on this way because it's not efficient (in the way that i will need to call my fonction 100 times
# if i have 100 files). I will browse the folder -> please see the file name: load_data_version_2




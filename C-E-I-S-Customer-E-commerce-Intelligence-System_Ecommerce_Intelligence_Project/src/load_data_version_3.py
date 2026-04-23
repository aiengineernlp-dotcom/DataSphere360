import pandas as pd
import os


def data_loading_with_pandas(folderpath: str) -> dict:
    data_frames = {}
    # 1. Vérifier si le dossier existe
    if not os.path.exists(folderpath):
        raise FileNotFoundError(f'Folder {folderpath} doest not exist')

    else:  # 2. Boucler sur tous les fichiers du dossier
        for filename in os.listdir(folderpath):
            if filename.endswith(".csv"):  # On ne traite que les fichiers .csv
                file_path = os.path.join(folderpath, filename)

                table_name = os.path.splitext(filename)[
                    0]  # On utilise le nom du fichier sans l'extension comme clé (ex: 'customers')
                try:
                    df = pd.read_csv(file_path)
                    data_frames[table_name] = df
                    print(f"✅ {filename} chargé (Table: {table_name}")
                except Exception as e:
                    print(f"❌ Erreur lors du chargement de {filename}: {e}")

    return data_frames


# Utilisation
path = "../python_project_aiml_logicmojo_dataset/"
all_data = data_loading_with_pandas(path)
print(all_data['customers'].head())






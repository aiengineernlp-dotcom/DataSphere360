import pandas as pd
import os


def load_all_csv_from_folder(folder_path: str) -> dict:
    data_frames = {}

    # 1. Vérifier si le dossier existe
    if not os.path.exists(folder_path):
        print(f"Le dossier {folder_path} n'existe pas.")
        return data_frames

    # 2. Boucler sur tous les fichiers du dossier
    for filename in os.listdir(folder_path):
        # On ne traite que les fichiers .csv
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)

            # On utilise le nom du fichier sans l'extension comme clé (ex: 'customers')
            table_name = os.path.splitext(filename)[0]

            try:
                df = pd.read_csv(file_path)
                data_frames[table_name] = df
                print(f"✅ {filename} chargé (Table: {table_name})")
            except Exception as e:
                print(f"❌ Erreur lors du chargement de {filename}: {e}")

    return data_frames


# Utilisation
path = '../python_project_aiml_logicmojo_dataset/'
all_data = load_all_csv_from_folder(path)

# Maintenant, tu peux accéder à tes données facilement :
# print(all_data['customers'].head())

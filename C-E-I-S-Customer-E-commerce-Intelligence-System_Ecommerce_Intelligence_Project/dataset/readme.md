Note:

Si les donnees sont generees, a partir de la fonction de generation, tu les envoient directement dans analysis puis dans psql si vraiment c'est nesessaire (GROS DONNEES).

Si les donnees sont brutement  recues de diffrentes sources comme sur le project Datasphere, cela signifie que c'est en local. 
Alors, a partir du dossier dataset, tu crees une fonction push_data_csv_to_psql(). Et desormais partout ou je veux les utilises, je fais le fetch_data_from_psql(). 
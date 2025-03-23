import json
from flask import Flask, request, jsonify
from google.cloud import bigquery
import pandas as pd
import re
import os


app = Flask(__name__)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './{ServiceAccountKey}.json'
client = bigquery.Client()

def nettoyer_noms_colonnes(columns):
    """
    Nettoie les noms de colonnes pour respecter les conventions de BigQuery.
    """
    cleaned_columns = []
    for col in columns:
        clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', col.lower())
        clean_col = re.sub(r'^[^a-zA-Z_]+', '_', clean_col)
        cleaned_columns.append(clean_col[:300])
    return cleaned_columns

@app.route('/upload/csv', methods=['POST'])
def upload_csv():
    try:
        file = request.files.get('file')
        dataset_id = request.form.get('dataset_id')
        table_name = request.form.get('table_name')
        use_header = request.form.get('use_header', 'true').lower() == 'true'
        columns = request.form.get('columns') 
        rename_columns = request.form.get('rename_columns')
        
        if not file:
            return jsonify({"error": "Fichier CSV non fourni"}), 400

        if use_header:
            df = pd.read_csv(file)
        else:
            temp_df = pd.read_csv(file, header=None)
            df = pd.read_csv(file, skiprows=1, header=None)
            df.columns = temp_df.iloc[0].values

        current_headers = df.columns.tolist()
        renamed_headers = nettoyer_noms_colonnes(current_headers)

        # **Étape 1 : Prévisualisation des colonnes et de leur renommage**
        if not dataset_id or not table_name:
            return jsonify({
                "message": "Prévisualisation des colonnes pour BigQuery",
                "current_headers": current_headers,
                "renamed_headers": renamed_headers,
                "instruction": "Ajoutez dataset_id et table_name pour uploader dans BigQuery"
            }), 200

        # **Étape 2 : Sélection des colonnes à extraire**
        # Chaines de caractères séparées par des virgules
        # Si des colonnes sont spécifiées, on les extrait sinon on garde toutes les colonnes
        if columns:
            selected_columns = columns.split(",")
            df = df[selected_columns]

        # **Étape 3 : Renommage des colonnes**
        # Dictionnaire JSON avec les anciens noms de colonnes en clé et les nouveaux noms en valeur
        # Si des colonnes sont spécifiées, on les renomme sinon on garde les nommage de BigQuery
        if rename_columns:
            rename_dict = json.loads(rename_columns)
            df = df.rename(columns=rename_dict)

        df.columns = nettoyer_noms_colonnes(df.columns)
        table_id = f"{client.project}.{dataset_id}.{table_name}"

        job = client.load_table_from_dataframe(df, table_id)
        job.result()

        return jsonify({"message": "Données CSV importées avec succès dans BigQuery !"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
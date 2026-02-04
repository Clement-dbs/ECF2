# ECF - Analyse et prédiction de la consommation énergétique des bâtiments publics

Pipeline complet permettant d’ingérer, nettoyer, analyser et visualiser les données de consommation d’électricité, gaz et eau des bâtiments publics, enrichies par les données météorologiques et le référentiel des bâtiments.

##  Structure du projet
```
ecf_energie/
├── README.md                               
├── data_ecf/
│   ├── batiments.csv
│   ├── consommations_raw.csv
│   ├── consommations_raw_lite.csv
│   ├── meteo_raw.csv
│   └── tarifs_energie.csv
├── notebooks/
│   ├── 01_exploration_spark.ipynb
│   ├── 02_nettoyage_spark.py
│   ├── 03_agregations_spark.py
│   ├── 04_nettoyage_meteo_pandas.ipynb
│   ├── 05_fusion_enrichissement.py
│   ├── 06_statistiques_descriptives.py
│   ├── 07_analyse_correlations.py
│   ├── 08_detection_anomalies.py
│   ├── 09_visualisations_matplotlib.ipynb
│   ├── 10_visualisations_seaborn.ipynb
│   └── 11_dashboard_executif.ipynb
├── output/
│   ├── consommations_clean/              
│   ├── consommations_agregees/
│   ├── meteo_clean.csv
│   ├── consommations_enrichies.csv
│   ├── consommations_enrichies/
│   ├── matrice_correlation.csv
│   ├── figures/                           
│   └── tableau_de_synthese.md
├── requirements.txt
├── docker_compose.yml
├── generate_data_ecf.py
├── ECF_CONCEPTEUR_APPLI_DONNEES.md


```

## Installation et dépendances


Créer un environnement virtuel python :
```
    python3 -m venv .venv
    venv\Scripts\Activate.ps1      
```

Installer les dépendances :
```
    pip install -r requirements.txt
```

Executer le docker-compose.yml
```
    docker compose up -d
```

## Utilisation

• Exécuter les notebooks dans l’ordre 

• Vérifier les fichiers Parquet dans output/consommations_clean avant de lancer Pandas

• Inspecter les logs dans 02_nettoyage_spark.py pour connaître les lignes supprimées ou corrigées

• Les figures sont sauvegarder sous output/figures 

• Ne pas modifier les dossiers data/ et output/ manuellement pour éviter les conflits avec Spark

#!/usr/bin/env python
# coding: utf-8

# #### Etape 3.3 : Detection d'anomalies
# - Identifier les pics de consommation anormaux (>3 ecarts-types)
# - Detecter les periodes de sous-consommation suspectes (batiment ferme non declare)
# - Reperer les batiments dont la consommation ne correspond pas a leur DPE
# - Lister les batiments necessitant un audit energetique
# 
# **Livrables** :
# - Notebook `08_detection_anomalies.ipynb`
# - Liste des anomalies `output/anomalies_detectees.csv`
# - Rapport de recommandations d'audit
# 

# In[4]:


import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("08_detection_anomalies") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/output/consommations_enrichies")
df = df.toPandas()

#- Identifier les pics de consommation anormaux (>3 ecarts-types)
df_pivot = df.pivot_table(
    values="consommation",
    index=["timestamp", "batiment_id"],
    columns="type_energie",
    aggfunc="sum",
    fill_value=0
).reset_index()

type_energies = ["eau", "gaz", "electricite"]

for col in type_energies:
    if col in df_pivot.columns:
        mean_val = df_pivot[col].mean()
        std_val = df_pivot[col].std()
        df_pivot["anomalie_" + col] = ((df_pivot[col] - mean_val).abs() > 3 * std_val)
        n_anomalies = df_pivot["anomalie_" + col].sum()
        print(f"{col} : {n_anomalies} pics anormaux")

# - Detecter les sous-consommation suspectes (batiment ferme non declare)
seuil_sous_consommation = 0.1  # kWh
for col in type_energies:
    if col in df_pivot.columns:
        df_pivot["sous_consommation_" + col] = (df_pivot[col] < seuil_sous_consommation)
        n_sous_consommations = df_pivot["sous_consommation_" + col].sum()
        print(f"{col} : {n_sous_consommations} sous-consommation suspectes")


# - Liste des anomalies `output/anomalies_detectees.csv`
anomalies = df_pivot[df_pivot.filter(regex="anomalie_|sous_consommation_").any(axis=1)]
anomalies.to_csv("/output/anomalies_detectees.csv", index=False)


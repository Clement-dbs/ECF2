#!/usr/bin/env python
# coding: utf-8

# #### Etape 2.2 : Fusion et enrichissement
# - Charger les consommations nettoyees (depuis Parquet)
# - Fusionner avec les donnees meteo (sur commune et timestamp arrondi a l'heure)
# - Fusionner avec le referentiel batiments
# - Fusionner avec les tarifs pour calculer le cout financier
# - Creer des features derivees :
#   - Consommation par occupant
#   - Consommation par m2
#   - Cout journalier, mensuel, annuel
#   - Indice de performance energetique (IPE)
#   - Ecart a la moyenne de la categorie
# 
# **Livrables** :
# - Notebook `05_fusion_enrichissement.ipynb`
# - Dataset final `output/consommations_enrichies.csv` et `.parquet`
# - Dictionnaire de donnees (description de toutes les colonnes)

# In[ ]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("local") \
    .appName("05_fusion_enrichissement") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Imports
df_consommation = spark.read.parquet("/output/consommation_clean")

df_batiments = spark.read \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .option("sep", ",") \
          .csv("/data_ecf/batiments.csv")

df_meteo = spark.read.csv("/output/meteo_clean.csv", header=True, inferSchema=True)

df_tarifs = spark.read \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .option("sep", ",") \
          .csv("/data_ecf/tarifs_energie.csv")

print("Aperçu des données chargées :")

print(df_consommation.show(5))
print("----")
print("Batiments")
print(df_batiments.show(5))
print("----")
print("Météo")
print(df_meteo.show(5))
print("----")
print("Tarifs")
print(df_tarifs.show(5))

# Fusionner avec les donnees meteo (sur commune et timestamp arrondi a l'heure)
# Créer une colonne "timestamp_hour" arrondie à l'heure

# Fusionner sur commune et heure
df_fusion = df_consommation.join(
    df_meteo,
    on=["timestamp"],
    how="left"
)

print("Après fusion avec météo")
print(df_fusion.show(5))

# Fusionner avec le referentiel batiments
df_fusion = df_fusion.join(
    df_batiments,
    on=["commune", "batiment_id"],
    how="left"
)

# Fusionner avec les tarifs pour calculer le cout financier
df_fusion = df_fusion.join(
    df_tarifs,
    on=["type_energie"],
    how="left"
)


print(f"Fusion terminé : {df_fusion}")
print(df_fusion.show(5))


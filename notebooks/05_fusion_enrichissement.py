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

# Fusionner les données
df_fusion = df_consommation.join(df_batiments, on="batiment_id", how="left") \
                           .join(df_meteo, on=["commune", "timestamp"], how="left") \
                           .join(df_tarifs, on="type_energie", how="left")

#  Creer des features derivees :
df_fusion = df_fusion.withColumn("consommation_par_occupant", F.round(F.col("consommation") / F.col("nb_occupants_moyen"),2)) \
                     .withColumn("consommation_par_m2", F.round(F.col("consommation") / F.col("surface_m2"),2)) \
                     .withColumn("ipe", F.round(F.col("consommation") / F.col("surface_m2") * 1000,2))

# Cout journalier, mensuel, annuel
df_cout_journalier = df_fusion.groupBy("batiment_id","type_energie","date") \
    .agg(F.round(F.sum("consommation"),2).alias("consommation_journaliere"),
         F.round(F.sum(F.col("consommation")*F.col("tarif_unitaire")),2).alias("cout_journalier"))

df_cout_mensuel = df_fusion.groupBy("batiment_id","type_energie","annee","mois") \
    .agg(F.round(F.sum("consommation"),2).alias("consommation_mensuelle"),
         F.round(F.sum(F.col("consommation")*F.col("tarif_unitaire")),2).alias("cout_mensuel"))

df_cout_annuel = df_fusion.groupBy("batiment_id","type_energie","annee") \
    .agg(F.round(F.sum("consommation"),2).alias("consommation_annuelle"),
         F.round(F.sum(F.col("consommation")*F.col("tarif_unitaire")),2).alias("cout_annuel"))

df_final = df_fusion.join(df_cout_journalier, on=["batiment_id","type_energie","date"], how="left") \
                    .join(df_cout_mensuel, on=["batiment_id","type_energie","annee","mois"], how="left") \
                    .join(df_cout_annuel, on=["batiment_id","type_energie","annee"], how="left")

# --- Sauvegarde ---
df_final.write.mode("overwrite").parquet("/output/consommations_enrichies")
df_final.toPandas().to_csv("/output/consommations_enrichies.csv", index=False)

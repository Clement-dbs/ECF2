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

# Fusionner avec le referentiel batiments
df_fusion = df_consommation.join(
    df_batiments,
    on=["batiment_id"],
    how="left"
)

# Fusionner avec les donnees meteo (sur commune et timestamp arrondi a l'heure)
df_fusion = df_fusion.join(
    df_meteo,
    on=["commune", "timestamp"],
    how="left"
)

# Fusionner avec les tarifs pour calculer le cout financier
df_fusion = df_fusion.join(
    df_tarifs,
    on=["type_energie"],
    how="left"
)

print(df_fusion.show(5))


# - Creer des features derivees :

#   - Consommation par occupant
df_consommation_par_occupant = df_fusion.withColumn("consommation_par_occupant", F.col("consommation") / F.col("nb_occupants"))
#   - Consommation par m2
df_consommation_par_m2 = df_consommation_par_occupant.withColumn("consommation_par_m2", F.col("consommation") / F.col("surface"))
#   - Cout journalier, mensuel, annuel
df_cout_journalier = df_consommation_par_m2.withColumn("cout_journalier", F.col("consommation") * F.col("tarif_kwh"))
df_cout_mensuel = df_cout_journalier.withColumn("cout_mensuel", F.col("cout_journalier") * 30)
df_cout_annuel = df_cout_mensuel.withColumn("cout_annuel", F.col("cout_mensuel") * 12)

#   - Indice de performance energetique (IPE)
df_ipe = df_cout_journalier.withColumn("ipe", F.col("consommation") / F.col("surface") * 1000)


print("Résultats des features dérivées :")
print("Consommation par occupant")
print(df_consommation_par_occupant.show(5))
print("Consommation par m2")
print(df_consommation_par_m2.show(5))
print("Cout journalier")
print(df_cout_journalier.show(5))
print("Cout mensuel")
print(df_cout_mensuel.show(5))
print("Cout annuel")
print(df_cout_annuel.show(5))
print("IPE")
print(df_ipe.show(5))

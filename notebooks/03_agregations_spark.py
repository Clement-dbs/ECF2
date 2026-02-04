from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import os

path = "/output/consommation_clean"
print("Contenu du dossier racine:", os.listdir(path))

# Lister récursivement les fichiers
for root, dirs, files in os.walk(path):
    for f in files:
        print(root, f)


spark = SparkSession.builder \
    .master("local") \
    .appName("03_agregations") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


df_batiments = spark.read \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .option("sep", ",") \
          .csv("/data_ecf/batiments.csv")


df_batiments.show(5)
# Charger les donnees de consommation avec PySpark
df_consommation_parquet = spark.read.parquet("/output/consommation_clean/*")

df_consommation_parquet.limit(5).show()


# - Joindre consommations avec referentiel batiments
print("Jointure df_consommations avec  df_batiments")
df = df_consommation_parquet.join(df_batiments, on="batiment_id", how="left")
df.limit(5).show()

# - Calculer l'intensite energetique (kWh/m2)
print("Calculer l'intensite energetique (kWh/m2)")
df = df.withColumn("intensite_energetique", F.when(F.col("surface_m2") > 0, F.round(F.col("consommation") / F.col("surface_m2"), 2)).otherwise(None))

df.limit(5).show()


# - Identifier les batiments hors norme (>3x la mediane de leur categorie)


# - Calculer les totaux par commune et par type de batiment 
df_totaux_communes = df.groupBy("commune", "type").agg(F.round(F.sum("consommation"), 2).alias("total_consommation"))
df_totaux_communes.limit(5).show()

# - Creer une vue SQL exploitable


# Sauvegarder en Parquet partitionne par date et type d'energie
df = df.withColumn("date", F.to_date("timestamp"))

df.write.mode("overwrite") \
    .partitionBy("date", "type_energie") \
    .parquet("/output/consommations_agregees")
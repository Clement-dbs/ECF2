# #### Etape 1.2 : Pipeline de nettoyage Spark
# 
# **Competence evaluee : C2.2 - Traiter des donnees structurees avec un langage de programmation**
# 
# - Creer des UDF pour parser les timestamps multi-formats
# - Convertir les valeurs avec virgule en float
# - Filtrer les valeurs negatives et les outliers (>10000)
# - Dedupliquer sur (batiment_id, timestamp, type_energie)
# - Calculer les agregations :
#   - Consommations horaires moyennes par batiment
#   - Consommations journalieres par batiment et type d'energie
#   - Consommations mensuelles par commune
# - Sauvegarder en Parquet partitionne par date et type d'energie
# 
# **Livrables** :
# - Script `02_nettoyage_spark.py` (executable en ligne de commande)
# - Fichiers Parquet dans `output/consommations_clean/`
# - Log de traitement (lignes en entree/sortie, lignes supprimees)


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.types import TimestampType

spark = SparkSession.builder.master("spark://spark-master:7077").appName("02_nettoyage").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Charger les donnees de consommation avec PySpark
df_consommation = spark.read \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .option("sep", ",") \
          .csv("/data_ecf/consommations_raw_lite.csv")

df_batiments = spark.read \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .option("sep", ",") \
          .csv("/data_ecf/batiments.csv")

# Creer des UDF pour parser les timestamps multi-formats
def parse_timestamp_udf_func(ts):
    if ts is None:
        return None

    ts = ts.strip()

    formats = [
        "%Y-%m-%d %H:%M:%S",      
        "%Y-%m-%dT%H:%M:%S",     
        "%d/%m/%Y %H:%M",        
        "%m/%d/%Y %H:%M:%S"      
    ]

    for fmt in formats:
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            pass

    return None

parse_timestamp_udf = F.udf(parse_timestamp_udf_func, TimestampType())

df_consommation = df_consommation.withColumn("timestamp", parse_timestamp_udf(F.col("timestamp"))
)

# df_consommation.show(10)

# Convertir les valeurs avec virgule en float
print("Convertir les valeurs avec virgule en float")
df_consommation = df_consommation.withColumn("consommation", F.regexp_replace(F.col("consommation"), ",", ".").cast("float"))
# df_consommation.show(10)


# Filtrer les valeurs negatives et les outliers (>10000)
print("Filtrer les valeurs negatives et les outliers (>10000)")
df_consommation = df_consommation.filter((F.col("consommation") >= 0) & (F.col("consommation") <= 10000))
# df_consommation.show(10)


# Dedupliquer sur (batiment_id, timestamp, type_energie)
print("Dedupliquer sur (batiment_id, timestamp, type_energie)")
df_consommation = df_consommation.dropDuplicates(["batiment_id", "timestamp", "type_energie"])
df_consommation.show(10)

# Sauvegarder en Parquet partitionne par date et type d'energie
df_consommation = df_consommation.withColumn("date", F.to_date("timestamp"))

df_consommation.write.mode("overwrite") \
    .partitionBy("date", "type_energie") \
    .parquet("/output/consommation_clean")

# Calculer les agregations :

# Jointure avec df_batiments et df_consommation 
df_consommation = df_consommation.join(df_batiments, on="batiment_id", how="left")
# df_consommation.show(10)

# Consommations horaires moyennes par batiment
print("Consommation horaire")
df_consommation_horraire = df_consommation.groupBy("batiment_id", "unite",F.hour("timestamp").alias("heure")) \
    .agg(F.round(F.avg("consommation"), 2).alias("consommation_horaire_moyenne"))
# df_consommation_horraire.show(10)

# Consommations journalieres par batiment et type d'energie
print("Consommation quotidienne")
df_consommation_quotidiene = df_consommation.groupBy("batiment_id", "type_energie","unite", F.to_date("timestamp").alias("date")) \
    .agg(F.sum("consommation").alias("consommation_quotidienne"))
# df_consommation_quotidiene.show(10)

# Consommations mensuelles par commune
print("Consommation mensuelle")
df_consommation_mensuelle = df_consommation.groupBy("commune", "unite",F.date_format("timestamp", "MMMM").alias("mois")) \
    .agg(F.sum("consommation").alias("consommation_mensuelle"))
# df_consommation_mensuelle.show(10)



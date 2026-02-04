import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("07_analyse_correlations") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df_spark = spark.read.parquet("/output/consommations_enrichies")
df = df_spark.toPandas()

df_pivot = df.pivot_table(
    values="consommation",
    index=["timestamp", "batiment_id"],
    columns="type_energie",
    aggfunc="sum",
    fill_value=0   
).reset_index()

meteo_cols = ["temperature_c", "humidite_pct", "rayonnement_solaire_wm2","vitesse_vent_kmh", "precipitation_mm"]

batiment_cols = ["surface_m2", "nb_occupants_moyen", "annee_construction"]

df_context = df[["timestamp", "batiment_id"] + meteo_cols + batiment_cols].drop_duplicates()

df_corr = df_pivot.merge(df_context, on=["timestamp", "batiment_id"], how="left")

corr_cols = [c for c in ["eau", "gaz", "electricite"] + meteo_cols + batiment_cols if c in df_corr.columns]

df_corr_clean = df_corr[corr_cols].dropna()

correlation_matrix = df_corr_clean.corr().round(3)

print("\nMatrice de corrélation :")
print(correlation_matrix)

print("\nCorrélations significatives (>0.5 ou <-0.5) :")

# Fonction pour déterminer la force de la corrélation
def corr_strength(corr_val):
    if abs(corr_val) > 0.5:
        return "forte"
    elif abs(corr_val) > 0.3:
        return "moderee"
    else:
        return "faible"

# Colonnes
available_energies = [c for c in ["eau", "gaz", "electricite"] if c in df_corr_clean.columns]
available_meteo = [c for c in meteo_cols if c in df_corr_clean.columns]
available_batiments = [c for c in batiment_cols if c in df_corr_clean.columns]

# Corrélations énergies avec la météo
print("\nCorrelations énergies vs meteo:")
for energy in available_energies:
    print(f"\n{energy}:")
    for meteo in available_meteo:
        corr = correlation_matrix.loc[energy, meteo]
        print(f"  vs {meteo}: {corr:+.3f} ({corr_strength(corr)})")

# Corrélations énergies avec les caractéristiques des bâtiment
print("\nCorrelations énergies vs caractéristiques bâtiment:")
for energy in available_energies:
    print(f"\n{energy}:")
    for bat in available_batiments:
        corr = correlation_matrix.loc[energy, bat]
        print(f"  vs {bat}: {corr:+.3f} ({corr_strength(corr)})")

# Matrice de correlation exportee `output/matrice_correlation.csv`
correlation_matrix.to_csv("/output/matrice_correlation.csv")
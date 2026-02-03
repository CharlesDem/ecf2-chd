import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType
from datetime import datetime
import os



SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")
OUTPUT_LOGS = os.path.join(SCRIPT_DIR, "..", "output", "logs")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "output", "consommation_clean")

CONSOMMATION_PATH = os.path.join(DATA_DIR, "consommations_raw.csv")
BATIMENTS_PATH = os.path.join(DATA_DIR, "batiments.csv")

logging.basicConfig(
    filename=f"{OUTPUT_LOGS}/02_nettoyage.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logging.info("Traitement démarré")

def parse_multi_format_timestamp(timestamp_str):

    if timestamp_str is None:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue

    return None

def clean_conso(value_str):

    if value_str is None:
        return None

    try:
        clean_str = value_str.replace(",", ".")
        return float(clean_str)
    except (ValueError, AttributeError):
        return None

def show_df_in_log(df):
    df.show()
    rows = df.limit(20).collect()
    for row in rows:
        logging.info(row)


def main():
    spark = SparkSession.builder \
        .appName("ECF 2 batiments - Nettoyage") \
        .master("local[4]") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.files.maxPartitionBytes", "128m") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    timestamp_udf = F.udf(parse_multi_format_timestamp, TimestampType())
    clean_conso_udf = F.udf(clean_conso, DoubleType())

    # Charger les donnees brutes
    logging.info("\n[1/6] Chargement des donnees brutes...")
    df_raw = spark.read \
        .option("header", "true") \
        .csv(CONSOMMATION_PATH)

    initial_count = df_raw.count()


    # Parser les timestamps 
    logging.info("\n[2/6] Parsing des timestamps multi-formats...")
    df_timestamp_clean = df_raw.withColumn(
        "timestamp_parsed",
        timestamp_udf(F.col("timestamp"))
    )

    # Filtrer les timestamps invalides
    invalid_timestamps = df_timestamp_clean.filter(F.col("timestamp_parsed").isNull()).count()
    df_timestamp_clean = df_timestamp_clean.filter(F.col("timestamp_parsed").isNotNull())
    logging.info(f"  Timestamps invalides supprimes: {invalid_timestamps:,}")


    logging.info("\n[3/6] Conversion des valeurs numeriques...")
    df_with_clean_conso = df_timestamp_clean.withColumn(
        "conso_clean",
        clean_conso_udf(F.col("consommation"))
    )

        # Filtrer les valeurs non numeriques
    invalid_values = df_with_clean_conso.filter(F.col("conso_clean").isNull()).count()
    df_with_clean_conso = df_with_clean_conso.filter(F.col("conso_clean").isNotNull())
    logging.info(f"  Valeurs non numeriques supprimees: {invalid_values:,}")


    # Supprimer les valeurs negatives et les outliers (>10000)
    logging.info("\n[4/6] Suppression des valeurs aberrantes...")
    negative_count = df_with_clean_conso.filter(F.col("conso_clean") < 0).count()
    outlier_count = df_with_clean_conso.filter(F.col("conso_clean") > 10000).count()

    df_clean = df_with_clean_conso.filter(
        (F.col("conso_clean") >= 0) & (F.col("conso_clean") <= 10000)
    )
    logging.info(f"  Valeurs negatives supprimees: {negative_count:,}")
    logging.info(f"  Outliers (>10000) supprimes: {outlier_count:,}")

    # Dedupliquer sur ("batiment_id", "timestamp", "type_energie")
    logging.info("\n[5/6] Deduplication...")
    before_dedup = df_clean.count()
    df_dedup = df_clean.dropDuplicates(["batiment_id", "timestamp", "type_energie"])
    after_dedup = df_dedup.count()
    duplicates_removed = before_dedup - after_dedup
    logging.info(f"  Doublons supprimes: {duplicates_removed:,}")

    logging.info("\n[6/6] Agregation horaire et sauvegarde...")

    # Ajouter les colonnes de temps
    df_with_time = df_dedup.withColumn(
        "date", F.to_date(F.col("timestamp_parsed"))
    ).withColumn(
        "hour", F.hour(F.col("timestamp_parsed"))
    ).withColumn(
        "year", F.year(F.col("timestamp_parsed"))
    ).withColumn(
        "month", F.month(F.col("timestamp_parsed"))
    )




    logging.info("*********************")
    logging.info("Consommation par heure et bâtiment (moyenne)")

    df_conso_hourly_avg = df_with_time.groupBy(
        "batiment_id", "type_energie", "unite", "date", "hour"
    ).agg(
        F.round(F.avg("conso_clean"), 2).alias("conso_avg"),
    ).orderBy("batiment_id", "date", "hour", "type_energie")

    show_df_in_log(df_conso_hourly_avg)



    logging.info("*********************")
    logging.info("Consommation totale par type de batiment et d'énergie")

    df_conso_daily_by_bat_and_type = df_with_time.groupBy(
        "batiment_id", "type_energie", "unite", "date"
    ).agg(
        F.round(F.sum("conso_clean"), 2).alias("conso_sum"),
    ).orderBy("batiment_id", "date", "type_energie")


    show_df_in_log(df_conso_daily_by_bat_and_type)

    # joindre batiments.csv

    df_batiments = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(BATIMENTS_PATH)

    df_final = df_with_time.join(
        df_batiments.select("batiment_id", "nom", "type", "commune", "surface_m2", "annee_construction", "classe_energetique", "nb_occupants_moyen"),
        on="batiment_id",
        how="left"
    )

    logging.info("*********************")
    logging.info("Consommation moyenne par commune")

    df_conso_monthly_by_commune = df_final.groupBy(
        "commune", "type_energie", "month"
    ).agg(
        F.round(F.sum("conso_clean"), 2).alias("conso_sum"),
    ).orderBy("commune", "month", "type_energie")


    show_df_in_log(df_conso_monthly_by_commune)

    logging.info("*********************")
    logging.info("Sauvegarde en parquet")

    final_count = df_final.count()

    logging.info("Nettoyage des cols inutiles")
    cols_to_drop = [
        "timestamp",
        "timestamp_parsed",
        "consommation"
    ]

    df_final = df_final.drop(*cols_to_drop)

    df_final.write \
        .mode("overwrite") \
        .partitionBy("date", "type_energie") \
        .parquet(f"{OUTPUT_DIR}")

    # Rapport final
    logging.info("RAPPORT DE NETTOYAGE")
    logging.info(f"Lignes en entree:              {initial_count:>12,}")
    logging.info(f"Timestamps invalides:          {invalid_timestamps:>12,}")
    logging.info(f"Valeurs non numeriques:        {invalid_values:>12,}")
    logging.info(f"Valeurs negatives:             {negative_count:>12,}")
    logging.info(f"Outliers (>10000):              {outlier_count:>12,}")
    logging.info(f"Doublons:                      {duplicates_removed:>12,}")
    total_removed = invalid_timestamps + invalid_values + negative_count + outlier_count + duplicates_removed
    logging.info(f"Total lignes supprimees:       {total_removed:>12,}")
    logging.info(f"Lignes apres agregation:       {final_count:>12,}")
    logging.info(f"\nFichiers Parquet sauvegardes dans: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType
from datetime import datetime
import os
import sys



SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "output", "consommation_clean")

CONSOMMATION_PATH = os.path.join(DATA_DIR, "consommations_raw.csv")
BATIMENTS_PATH = os.path.join(DATA_DIR, "batiments.csv")


def create_spark_session():
    """Cree et configure la session Spark."""
    spark = SparkSession.builder \
        .appName("TP Qualite Air - Nettoyage") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    # Creer la session Spark
    spark = create_spark_session()
    print(f"Spark version: {spark.version}")

    # # Enregistrer les UDFs
    # parse_timestamp_udf = F.udf(parse_multi_format_timestamp, TimestampType())
    # clean_value_udf = F.udf(clean_value, DoubleType())

    # Charger les donnees brutes
    print("\n[1/6] Chargement des donnees brutes...")
    df_raw = spark.read \
        .option("header", "true") \
        .csv(CONSOMMATION_PATH)

    df_raw.show()

if __name__ == "__main__":
    main()
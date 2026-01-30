from os import environ as env

from pyspark.sql import SparkSession


def bronze_etl():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("energy_consumption_bronze")
             .getOrCreate())

    bronze_data_frame = (spark
                         .read
                         .option("header", "true")
                         .option("inferSchema", "true")
                         .csv(env.get("DATA")))

    (bronze_data_frame
     .write
     .mode("overwrite")
     .parquet(env.get("MEDALLION_BRONZE")))

    spark.stop()


if __name__ == "__main__":
    bronze_etl()

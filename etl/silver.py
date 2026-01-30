from os import environ as env

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lead


def silver_etl():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("energy_consumption_silver")
             .getOrCreate())

    silver_data_frame = (spark
                         .read
                         .parquet(env.get("MEDALLION_BRONZE")))

    silver_data_frame = (silver_data_frame.withColumn(
        colName="daily_kwh",
        col=lead("meter_kwh").over(Window.orderBy("year", "month", "day")) - col("meter_kwh"))
    )

    silver_data_frame = silver_data_frame.dropna()

    (silver_data_frame
     .write
     .mode("overwrite")
     .partitionBy("year", "month")
     .parquet(env.get("MEDALLION_SILVER")))

    spark.stop()


if __name__ == "__main__":
    silver_etl()

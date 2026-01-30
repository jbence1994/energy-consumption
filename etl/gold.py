from os import environ as env

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, sum, avg, min, max
from pyspark.sql.types import DecimalType


def gold_etl():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("energy_consumption_gold")
             .getOrCreate())

    gold_data_frame = (spark
                       .read
                       .parquet(env.get("MEDALLION_SILVER")))

    gold_data_frame = gold_data_frame.groupBy("year", "month").agg(
        sum("daily_kwh").alias("sum_kwh"),
        round(avg("daily_kwh"), 2).alias("avg_kwh"),
        min("daily_kwh").alias("min_kwh"),
        max("daily_kwh").alias("max_kwh")
    )

    DISCOUNTED_KWH_PRICE_HUF = 36
    KWH_PRICE_HUF = 70
    KWH_DISCOUNT_LIMIT = 210

    gold_data_frame = (gold_data_frame.withColumn(
        colName="approx_total_price_huf",
        col=when(
            col("sum_kwh") > KWH_DISCOUNT_LIMIT,
            (KWH_DISCOUNT_LIMIT * DISCOUNTED_KWH_PRICE_HUF + (col("sum_kwh") - KWH_DISCOUNT_LIMIT) * KWH_PRICE_HUF)
        )
        .otherwise(col("sum_kwh") * DISCOUNTED_KWH_PRICE_HUF)
        .cast(DecimalType(10, 0))
    ).orderBy("year", "month"))

    (gold_data_frame
     .write
     .mode("overwrite")
     .parquet(env.get("MEDALLION_GOLD")))

    spark.stop()


if __name__ == "__main__":
    gold_etl()

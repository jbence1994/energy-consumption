from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max, min, round, sum, when
from pyspark.sql.types import DecimalType

from etl import config
from etl.helpers import get_spark_session, read_parquet, write_parquet

MONTHLY_DISCOUNTED_LIMIT_KWH = 210
DISCOUNTED_KWH_PRICE_HUF = 36
KWH_PRICE_HUF = 70


def gold_etl(spark: SparkSession = None) -> None:
    own_session = spark is None
    if own_session:
        spark = get_spark_session("energy_consumption_gold")
    try:
        df = read_parquet(spark, config.SILVER_PATH)

        df = df.groupBy("year", "month").agg(
            sum("daily_kwh").alias("sum_kwh"),
            round(avg("daily_kwh"), 2).alias("avg_kwh"),
            min("daily_kwh").alias("min_kwh"),
            max("daily_kwh").alias("max_kwh"),
        )

        df = df.withColumn(
            colName="approx_total_price_huf",
            col=when(
                col("sum_kwh") > MONTHLY_DISCOUNTED_LIMIT_KWH,
                (
                        MONTHLY_DISCOUNTED_LIMIT_KWH * DISCOUNTED_KWH_PRICE_HUF
                        + (col("sum_kwh") - MONTHLY_DISCOUNTED_LIMIT_KWH) * KWH_PRICE_HUF
                ),
            )
            .otherwise(col("sum_kwh") * DISCOUNTED_KWH_PRICE_HUF)
            .cast(DecimalType(10, 0)),
        ).orderBy("year", "month")

        write_parquet(df, config.GOLD_PATH)
    finally:
        if own_session:
            spark.stop()


if __name__ == "__main__":
    gold_etl()

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lead

from etl import config
from etl.helpers import get_spark_session, read_parquet, write_parquet_with_partitions


def silver_etl(spark: SparkSession = None) -> None:
    own_session = spark is None
    if own_session:
        spark = get_spark_session("energy_consumption_silver")
    try:
        df = read_parquet(spark, config.BRONZE_PATH)

        df = df.withColumn(
            colName="daily_kwh",
            col=lead("kwh").over(Window.orderBy("year", "month", "day")) - col("kwh"),
        )

        df = df.dropna()

        write_parquet_with_partitions(df, config.SILVER_PATH, "year", "month")
    finally:
        if own_session:
            spark.stop()


if __name__ == "__main__":
    silver_etl()

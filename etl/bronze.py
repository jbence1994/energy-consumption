from pyspark.sql import SparkSession

from etl import config
from etl.helpers import get_spark_session, read_csv, write_parquet


def bronze_etl(spark: SparkSession = None) -> None:
    own_session = spark is None
    if own_session:
        spark = get_spark_session("energy_consumption_bronze")
    try:
        df = read_csv(spark, config.DATA_PATH)
        write_parquet(df, config.BRONZE_PATH)
    finally:
        if own_session:
            spark.stop()


if __name__ == "__main__":
    bronze_etl()

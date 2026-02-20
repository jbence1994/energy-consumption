from typing import Union, List

from pyspark.sql import SparkSession, DataFrame


def get_spark_session(
        app_name: str,
        master: str = "local[*]"
):
    return (SparkSession.builder
            .master(master)
            .appName(app_name)
            .getOrCreate())


def read_csv(
        spark_session: SparkSession,
        csv_path: str
):
    return (spark_session
            .read
            .option(key="header", value="true")
            .option(key="inferSchema", value="true")
            .csv(csv_path))


def read_parquet(
        spark_session: SparkSession,
        parquet_path: str
):
    return (spark_session
            .read
            .parquet(parquet_path))


def write_parquet(
        data_frame: DataFrame,
        parquet_path: str,
        mode: str = "overwrite"
):
    (data_frame
     .write
     .mode(mode)
     .parquet(parquet_path))


def write_parquet_with_partitions(
        data_frame: DataFrame,
        parquet_path: str,
        *partition_columns: Union[str, List[str]],
        mode: str = "overwrite",
):
    (data_frame
     .write
     .mode(mode)
     .partitionBy(*partition_columns)
     .parquet(parquet_path))

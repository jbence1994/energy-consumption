from etl.bronze import bronze_etl
from etl.gold import gold_etl
from etl.helpers import get_spark_session
from etl.silver import silver_etl


def run() -> None:
    spark = get_spark_session("energy_consumption")
    try:
        bronze_etl(spark)
        silver_etl(spark)
        gold_etl(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()

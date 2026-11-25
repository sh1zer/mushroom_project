import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from mushroom.config import CATEGORICAL_FEATURES, MISSING, NUMERIC_FEATURES

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"


def get_spark(app_name: str, with_kafka: bool = False) -> SparkSession:
    builder = SparkSession.builder.appName(app_name).config("spark.sql.shuffle.partitions", "8")
    if "SPARK_MASTER" in os.environ:
        builder = builder.master(os.environ["SPARK_MASTER"])
    if with_kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def normalize_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.strip().lower().replace("-", "_") for c in df.columns])


def clean_features(df: DataFrame) -> DataFrame:
    for c in NUMERIC_FEATURES:
        df = df.withColumn(c, F.col(c).cast("double"))
    for c in CATEGORICAL_FEATURES:
        value = F.lower(F.trim(F.col(c)))
        df = df.withColumn(c, F.when(value.isNull() | (value == ""), MISSING).otherwise(value))
    return df

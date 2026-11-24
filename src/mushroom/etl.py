from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType

from mushroom import config as C
from mushroom.spark_utils import get_spark
from mushroom.transforms import clean_features, normalize_columns


def read_secondary(spark: SparkSession) -> DataFrame:
    df = normalize_columns(spark.read.option("header", True).option("sep", ";").csv(str(C.SECONDARY_CSV)))
    rows = df.rdd.zipWithIndex().map(lambda pair: tuple(pair[0]) + (pair[1],))
    df = spark.createDataFrame(rows, StructType(df.schema.fields + [StructField("row_id", LongType())]))
    return df.withColumn("species_id", (F.col("row_id") / C.ROWS_PER_SPECIES).cast("int"))


def build_clean(raw: DataFrame) -> DataFrame:
    return (
        clean_features(raw)
        .withColumn("label", (F.col("class") == "p").cast("double"))
        .dropDuplicates(["species_id"] + C.FEATURES)
        .select(["species_id", "class", "label"] + C.FEATURES)
    )


def run() -> None:
    spark = get_spark("mushroom-etl")
    clean = build_clean(read_secondary(spark))
    clean.write.mode("overwrite").partitionBy("class").parquet(str(C.MUSHROOMS_PARQUET))
    print(f"saved {clean.count()} rows to {C.MUSHROOMS_PARQUET}")
    spark.stop()


if __name__ == "__main__":
    run()

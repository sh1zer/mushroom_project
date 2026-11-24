from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from mushroom.config import CATEGORICAL_FEATURES, MISSING, NUMERIC_FEATURES


def normalize_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.strip().lower().replace("-", "_") for c in df.columns])


def clean_features(df: DataFrame) -> DataFrame:
    for c in NUMERIC_FEATURES:
        df = df.withColumn(c, F.col(c).cast("double"))
    for c in CATEGORICAL_FEATURES:
        value = F.lower(F.trim(F.col(c)))
        df = df.withColumn(c, F.when(value.isNull() | (value == ""), MISSING).otherwise(value))
    return df

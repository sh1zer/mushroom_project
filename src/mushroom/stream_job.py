import os

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from mushroom import config as C
from mushroom.spark_utils import get_spark
from mushroom.transforms import clean_features

SCHEMA = StructType(
    [StructField("event_id", StringType()), StructField("true_class", StringType())]
    + [StructField(c, DoubleType()) for c in C.NUMERIC_FEATURES]
    + [StructField(c, StringType()) for c in C.CATEGORICAL_FEATURES]
)


def score(kafka_df: DataFrame, model: PipelineModel) -> DataFrame:
    events = kafka_df.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"), "timestamp").select("e.*", "timestamp")
    return (
        model.transform(clean_features(events))
        .withColumn("p_poison", F.round(vector_to_array("probability")[1], 3))
        .withColumn("decision", F.when(F.col("p_poison") >= C.SAFETY_THRESHOLD, "NIE_JESC").otherwise("PRAWDOPODOBNIE_JADALNY"))
        .select("event_id", "timestamp", "true_class", "p_poison", "decision", *C.FEATURES)
    )


def run() -> None:
    spark = get_spark("mushroom-streaming", with_kafka=True)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    model = PipelineModel.load(str(C.BEST_MODEL_DIR))

    observations = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", C.TOPIC_OBSERVATIONS)
        .load()
    )
    predictions = score(observations, model)

    (
        predictions.select(F.col("event_id").alias("key"), F.to_json(F.struct(*predictions.columns)).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", C.TOPIC_PREDICTIONS)
        .option("checkpointLocation", str(C.CHECKPOINT_DIR / "predictions"))
        .start()
    )
    (
        predictions.withWatermark("timestamp", "1 minute")
        .groupBy(F.window("timestamp", "1 minute"), "decision")
        .count()
        .writeStream.format("console")
        .outputMode("update")
        .option("truncate", False)
        .start()
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()

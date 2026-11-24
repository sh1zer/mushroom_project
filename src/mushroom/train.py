import json

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from mushroom import config as C
from mushroom.spark_utils import get_spark

N_FOLDS = 4
TEST_SPECIES_SHARE = 0.2
THRESHOLDS = [i / 10 for i in range(1, 10)]


def auc_evaluator() -> BinaryClassificationEvaluator:
    return BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="probability")


def feature_stages() -> list:
    idx = [f"{c}_idx" for c in C.CATEGORICAL_FEATURES]
    ohe = [f"{c}_ohe" for c in C.CATEGORICAL_FEATURES]
    return [
        StringIndexer(inputCols=C.CATEGORICAL_FEATURES, outputCols=idx, handleInvalid="keep"),
        OneHotEncoder(inputCols=idx, outputCols=ohe, handleInvalid="keep"),
        VectorAssembler(inputCols=ohe + C.NUMERIC_FEATURES, outputCol="features", handleInvalid="keep"),
    ]


def models() -> dict:
    return {
        "LogisticRegression": LogisticRegression(maxIter=100),
        "DecisionTree": DecisionTreeClassifier(maxDepth=12, seed=C.SEED),
        "RandomForest": RandomForestClassifier(numTrees=100, maxDepth=12, seed=C.SEED),
        "GBT": GBTClassifier(maxIter=50, maxDepth=6, seed=C.SEED),
    }


def add_species_split(df: DataFrame) -> DataFrame:
    rank = Window.partitionBy("class").orderBy(F.xxhash64("species_id", F.lit(C.SEED)))
    species = (
        df.select("species_id", "class").distinct()
        .withColumn("rank", F.row_number().over(rank))
        .withColumn("is_test", F.col("rank") <= F.ceil(F.count("*").over(Window.partitionBy("class")) * TEST_SPECIES_SHARE))
        .withColumn("fold", (F.col("rank") % N_FOLDS).cast("int"))
        .select("species_id", "is_test", "fold")
    )
    return df.join(species, "species_id")


def evaluate(pred: DataFrame, threshold: float = 0.5) -> dict:
    flagged = vector_to_array("probability")[1] >= threshold
    counts = {(r["label"], r["flagged"]): r["count"] for r in pred.groupBy("label", flagged.alias("flagged")).count().collect()}
    tp, fn = counts.get((1.0, True), 0), counts.get((1.0, False), 0)
    tn, fp = counts.get((0.0, False), 0), counts.get((0.0, True), 0)
    return {
        "accuracy": round((tp + tn) / (tp + tn + fp + fn), 4),
        "recall_poisonous": round(tp / (tp + fn), 4),
        "specificity": round(tn / (tn + fp), 4),
        "poisonous_as_edible": fn,
    }


def compare(train: DataFrame, test: DataFrame) -> tuple:
    results, fitted = [], {}
    for name, model in models().items():
        fitted[name] = Pipeline(stages=feature_stages() + [model]).fit(train)
        pred = fitted[name].transform(test)
        results.append({"model": name, "auc": round(auc_evaluator().evaluate(pred), 4), **evaluate(pred)})
        print(results[-1], flush=True)
    return results, fitted


def tune_forest(train: DataFrame) -> list:
    rf = RandomForestClassifier(seed=C.SEED)
    grid = ParamGridBuilder().addGrid(rf.numTrees, [50, 100]).addGrid(rf.maxDepth, [8, 12]).build()
    cv = CrossValidator(estimator=Pipeline(stages=feature_stages() + [rf]), estimatorParamMaps=grid,
                        evaluator=auc_evaluator(), numFolds=N_FOLDS, foldCol="fold", seed=C.SEED)
    cv_model = cv.fit(train)
    return [{**{p.name: v for p, v in params.items()}, "cv_auc": round(auc, 4)}
            for params, auc in zip(grid, cv_model.avgMetrics)]


def run() -> None:
    spark = get_spark("mushroom-train")
    df = add_species_split(spark.read.parquet(str(C.MUSHROOMS_PARQUET))).cache()
    rnd_train, rnd_test = df.randomSplit([0.8, 0.2], seed=C.SEED)
    sp_train, sp_test = df.filter(~F.col("is_test")), df.filter(F.col("is_test"))

    random_results, random_models = compare(rnd_train, rnd_test)
    species_results, species_models = compare(sp_train, sp_test)
    grid_results = tune_forest(sp_train)

    known_pred = random_models["RandomForest"].transform(rnd_test).cache()
    unseen_pred = species_models["RandomForest"].transform(sp_test).cache()

    final_model = Pipeline(stages=feature_stages() + [models()["RandomForest"]]).fit(df)
    final_model.write().overwrite().save(str(C.BEST_MODEL_DIR))

    metrics = {
        "data": {
            "rows": df.count(),
            "random_split": [rnd_train.count(), rnd_test.count()],
            "species_split": [sp_train.select("species_id").distinct().count(), sp_test.select("species_id").distinct().count()],
            "species_split_rows": [sp_train.count(), sp_test.count()],
        },
        "random": random_results,
        "species_holdout": species_results,
        "tuning": grid_results,
        "threshold_curve": {
            "known": [{"threshold": t, **evaluate(known_pred, t)} for t in THRESHOLDS],
            "unseen": [{"threshold": t, **evaluate(unseen_pred, t)} for t in THRESHOLDS],
        },
        "model": {"name": "RandomForest", "numTrees": 100, "maxDepth": 12, "threshold": C.SAFETY_THRESHOLD},
    }
    C.METRICS_REPORT.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(metrics["threshold_curve"], indent=2))
    spark.stop()


if __name__ == "__main__":
    run()

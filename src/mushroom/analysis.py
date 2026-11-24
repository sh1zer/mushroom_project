import json
import math

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.stat import ChiSquareTest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from mushroom import config as C
from mushroom.spark_utils import get_spark

IDX = [f"{c}_idx" for c in C.CATEGORICAL_FEATURES]


def chi_square(df: DataFrame) -> list:
    indexer = StringIndexer(inputCols=C.CATEGORICAL_FEATURES, outputCols=IDX)
    assembler = VectorAssembler(inputCols=IDX, outputCol="cat_vec")
    vec = Pipeline(stages=[indexer, assembler]).fit(df).transform(df)
    res = ChiSquareTest.test(vec, "cat_vec", "label").first()
    n = df.count()
    rows = [{"feature": c, "chi2": round(float(res.statistics[i]), 1), "p_value": float(res.pValues[i]),
             "cramers_v": round(math.sqrt(res.statistics[i] / n), 3)}
            for i, c in enumerate(C.CATEGORICAL_FEATURES)]
    return sorted(rows, key=lambda r: -r["cramers_v"])


def association_rules(df: DataFrame) -> dict:
    items = [F.concat(F.lit(f"{c}="), F.col(c)) for c in C.CATEGORICAL_FEATURES] + [F.concat(F.lit("class="), F.col("class"))]
    baskets = df.select(F.array(*items).alias("items"))
    model = FPGrowth(itemsCol="items", minSupport=0.03, minConfidence=0.9).fit(baskets)
    rules = model.associationRules.filter((F.size("antecedent") <= 3) & F.col("consequent")[0].startswith("class="))
    out = {}
    for cls in ("p", "e"):
        candidates = (rules.filter(F.col("consequent")[0] == f"class={cls}")
                      .orderBy(F.desc("confidence"), F.size("antecedent"), F.desc("support")).limit(300).collect())
        kept = []
        for r in candidates:
            if len(kept) < 10 and not any(set(k["antecedent"]) <= set(r["antecedent"]) for k in kept):
                kept.append(r)
        out[cls] = [{"antecedent": sorted(r["antecedent"]), "confidence": round(r["confidence"], 3),
                     "support": round(r["support"], 3), "lift": round(r["lift"], 3)} for r in kept]
    return out


def run() -> None:
    spark = get_spark("mushroom-analysis")
    df = spark.read.parquet(str(C.MUSHROOMS_PARQUET)).cache()
    result = {
        "chi_square": chi_square(df),
        "association_rules": association_rules(df),
    }
    C.ANALYSIS_REPORT.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    run()

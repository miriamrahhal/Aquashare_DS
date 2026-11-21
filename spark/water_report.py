"""Season report with Apache Spark: batch analysis of the water records.

  spark-submit spark/water_report.py <data folder> <output folder>

The ledger answers "how much water does farm-a have NOW?". This job answers questions about
the whole history, which need to scan every record:
  1. per day and farm: share, used, sent, received, and how much of the share was used
  2. per day: how much of the reservoir limit was used, and how fair the use was (Jain's index)
  3. per farm over the season: average use, days the share ran out, water given vs received
  4. transfer flows: who helps whom, and how much
  5. a suggested share for next season, based on what each farm really needed
Spark splits each table into partitions and processes them in parallel; on a cluster the same
code runs on many machines without change.
"""
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


SCHEMAS = {   # fixed column types, so an empty table (e.g. no transfers yet) still works
    "daily_limits": "day DATE, limit_l LONG",
    "allocations": "day DATE, farm_id STRING, amount_l LONG",
    "consumption": "day DATE, farm_id STRING, amount_l LONG, log_index LONG",
    "transfers": "day DATE, sender STRING, recipient STRING, amount_l LONG, status STRING",
}


def load(spark, folder, name):
    return spark.read.csv(f"{folder}/{name}.csv", header=True, schema=SCHEMAS[name])


def main(data, out):
    spark = (SparkSession.builder.appName("AquaShare season report")
             .config("spark.sql.shuffle.partitions", "8")        # small data: 8 partitions are enough
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    limits = load(spark, data, "daily_limits")
    allocations = load(spark, data, "allocations").withColumnRenamed("amount_l", "allocated")
    consumption = load(spark, data, "consumption")
    transfers = load(spark, data, "transfers").filter(F.col("status") == "committed")

    used = consumption.groupBy("day", "farm_id").agg(F.sum("amount_l").alias("consumed"),
                                                      F.count("*").alias("irrigations"))
    sent = transfers.groupBy("day", F.col("sender").alias("farm_id")).agg(F.sum("amount_l").alias("sent"))
    received = transfers.groupBy("day", F.col("recipient").alias("farm_id")).agg(F.sum("amount_l").alias("received"))

    # 1. per day and farm
    farm_day = (allocations.join(used, ["day", "farm_id"], "left")
                .join(sent, ["day", "farm_id"], "left").join(received, ["day", "farm_id"], "left")
                .fillna(0, ["consumed", "irrigations", "sent", "received"])
                .withColumn("available", F.col("allocated") + F.col("received") - F.col("sent"))
                .withColumn("use_pct", F.round(100 * F.col("consumed") / F.col("available"), 1))
                .withColumn("ran_out", F.col("consumed") >= F.col("available") * 0.98)
                .orderBy("day", "farm_id")).cache()

    # 2. per day: reservoir use and fairness. Jain's index of each farm's use_pct: 1.0 = perfectly even
    daily = (farm_day.groupBy("day").agg(
                F.sum("consumed").alias("consumed"),
                F.round(F.pow(F.sum("use_pct"), 2) / (F.count("*") * F.sum(F.pow("use_pct", 2))), 3).alias("fairness"),
                F.sum(F.col("ran_out").cast("int")).alias("farms_ran_out"))
             .join(limits, "day")
             .withColumn("reservoir_use_pct", F.round(100 * F.col("consumed") / F.col("limit_l"), 1))
             .orderBy("day"))

    # 3. per farm over the season
    farms = (farm_day.groupBy("farm_id").agg(
                F.round(F.avg("use_pct"), 1).alias("avg_use_pct"),
                F.sum(F.col("ran_out").cast("int")).alias("days_ran_out"),
                F.sum("consumed").alias("total_consumed"),
                F.sum("sent").alias("total_sent"),
                F.sum("received").alias("total_received"),
                F.sum("irrigations").alias("irrigations"))
             .withColumn("net_given", F.col("total_sent") - F.col("total_received"))
             .orderBy("farm_id"))

    # 4. transfer flows
    flows = (transfers.groupBy("sender", "recipient")
             .agg(F.count("*").alias("transfers"), F.sum("amount_l").alias("litres"))
             .orderBy(F.desc("litres")))

    # 5. suggested share for next season: proportional to what each farm really used,
    #    scaled to the average daily limit
    avg_limit = limits.agg(F.avg("limit_l")).first()[0]
    need = farm_day.groupBy("farm_id").agg(F.avg("consumed").alias("avg_need"))
    total_need = need.agg(F.sum("avg_need")).first()[0] or 1
    current = (allocations.withColumn("rn", F.row_number().over(Window.partitionBy("farm_id").orderBy(F.desc("day"))))
               .filter("rn = 1").select("farm_id", F.col("allocated").alias("current_share")))
    suggestion = (need.join(current, "farm_id")
                  .withColumn("suggested_share", F.round(F.col("avg_need") / total_need * avg_limit).cast("long"))
                  .withColumn("change", F.col("suggested_share") - F.col("current_share"))
                  .select("farm_id", "current_share", F.round("avg_need").cast("long").alias("avg_daily_use"),
                          "suggested_share", "change")
                  .orderBy("farm_id"))

    n_days, n_events = limits.count(), consumption.count()
    print(f"\n=== AquaShare season report (Spark {spark.version}, {spark.sparkContext.defaultParallelism} "
          f"parallel tasks) ===")
    print(f"{n_days} days, {n_events} irrigation records, {transfers.count()} transfers\n")
    print("Per farm over the season:")
    farms.show(truncate=False)
    print("Who helps whom (transfer flows):")
    flows.show(truncate=False)
    print("Days with the lowest fairness (Jain's index, 1.0 = every farm used the same part of its share):")
    daily.orderBy("fairness").show(5, truncate=False)
    print("Suggested share for next season (based on real use):")
    suggestion.show(truncate=False)

    for name, df in {"farm_day": farm_day, "daily": daily, "farms": farms, "flows": flows,
                     "suggestion": suggestion}.items():
        df.coalesce(1).write.mode("overwrite").csv(f"{out}/{name}", header=True)
    print(f"Results written to {out}/ (farm_day, daily, farms, flows, suggestion)")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "spark/data", sys.argv[2] if len(sys.argv) > 2 else "spark/output")

# build_gold_local.py — Phase 3: build the gold business-ready layer locally
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
        .appName("phase3-gold")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver = spark.read.format("delta").load("data/silver_trips")

# Gold = business-ready summary per pickup zone (real data uses zone IDs, not zips).
gold = (
    silver
        .groupBy("PULocationID")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_miles"),
            F.round(F.avg("duration_min"), 1).alias("avg_minutes"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        )
        .orderBy(F.desc("trips"))
)

gold.write.format("delta").mode("overwrite").save("data/gold_zone_summary")

print("=== gold: busiest pickup zones ===")
spark.read.format("delta").load("data/gold_zone_summary").show(10, truncate=False)

# print("Spark UI is live at http://localhost:4040 — press Enter to quit")
# input()   # keeps the session (and the UI) alive until you press Enter
spark.stop()
# build_silver_local.py — Phase 3: clean and enrich bronze into the silver layer
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
        .appName("phase3-silver")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.format("delta").load("data/bronze_trips")

# Silver = cleaned + enriched. Rules adapted to the REAL column set.
silver = (
    bronze
        # keep only physically sensible rows
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_distance") < 100)     # drop extreme outliers
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("fare_amount") < 500)
        .filter(F.col("passenger_count") > 0)     # real data has 0-passenger junk rows
        # trip duration in minutes. timestamp_diff handles TIMESTAMP_NTZ correctly,
        # unlike casting to long (which only works on plain TIMESTAMP).
        .withColumn(
            "duration_min",
            F.timestamp_diff("SECOND", F.col("tpep_pickup_datetime"),
                             F.col("tpep_dropoff_datetime")) / 60
        )
        .filter(F.col("duration_min") > 0)
        .filter(F.col("duration_min") < 240)      # drop trips longer than 4 hours
        # keep only the columns we actually need downstream
        .select(
            "tpep_pickup_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "passenger_count",
            "duration_min",
        )
)

silver.write.format("delta").mode("overwrite").save("data/silver_trips")

bronze_count = bronze.count()
silver_count = spark.read.format("delta").load("data/silver_trips").count()
print(f"bronze: {bronze_count}  ->  silver: {silver_count}  (removed {bronze_count - silver_count})")

spark.stop()

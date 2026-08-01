# pipeline_k8s.py — bronze -> silver -> gold on Kubernetes, reading/writing MinIO (S3).
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
        .appName("lakehouse-pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

BUCKET = "s3a://lakehouse"

# --- Bronze: read raw parquet from MinIO, write back as Delta ---
bronze = spark.read.parquet(f"{BUCKET}/raw/yellow_tripdata_2024-01.parquet")
bronze.write.format("delta").mode("overwrite").save(f"{BUCKET}/bronze/trips")
print("bronze rows:", bronze.count())

# --- Silver: clean + enrich ---
silver = (
    spark.read.format("delta").load(f"{BUCKET}/bronze/trips")
        .filter(F.col("trip_distance") > 0).filter(F.col("trip_distance") < 100)
        .filter(F.col("fare_amount") > 0).filter(F.col("fare_amount") < 500)
        .filter(F.col("passenger_count") > 0)
        # duration in minutes. Cast to plain timestamp first (source is timestamp_ntz),
        # then diff via unix time — works on every Spark version.
        .withColumn("duration_min",
            (F.unix_timestamp(F.col("tpep_dropoff_datetime").cast("timestamp"))
             - F.unix_timestamp(F.col("tpep_pickup_datetime").cast("timestamp"))) / 60)
        .filter(F.col("duration_min") > 0).filter(F.col("duration_min") < 240)
        .select("tpep_pickup_datetime", "PULocationID", "DOLocationID",
                "trip_distance", "fare_amount", "tip_amount", "duration_min")
)
silver.write.format("delta").mode("overwrite").save(f"{BUCKET}/silver/trips")
print("silver rows:", silver.count())

# --- Gold: per-zone summary ---
gold = (
    spark.read.format("delta").load(f"{BUCKET}/silver/trips")
        .groupBy("PULocationID")
        .agg(F.count("*").alias("trips"),
             F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
             F.round(F.avg("duration_min"), 1).alias("avg_minutes"),
             F.round(F.avg("tip_amount"), 2).alias("avg_tip"))
        .orderBy(F.desc("trips"))
)
gold.write.format("delta").mode("overwrite").save(f"{BUCKET}/gold/zone_summary")
print("gold zones:", gold.count())
gold.show(10, truncate=False)

spark.stop()

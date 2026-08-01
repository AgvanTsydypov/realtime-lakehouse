# build_bronze_local.py — Phase 3: read real NYC Taxi parquet, build bronze locally
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Local Spark session with Delta enabled (same wiring as the demo).
builder = (
    SparkSession.builder
        .appName("phase3-bronze")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Bronze = raw ingestion: read the source file exactly as-is, no cleaning.
# Note: this is a file path, not a catalog table — there is no Unity Catalog here.
raw = spark.read.parquet("data/yellow_tripdata_2024-01.parquet")

print("=== schema (raw source) ===")
raw.printSchema()
print("row count:", raw.count())

# Write it unchanged as a local Delta table (a folder on disk).
(raw.write
    .format("delta")
    .mode("overwrite")
    .save("data/bronze_trips"))

print("=== bronze delta history ===")
DeltaTable.forPath(spark, "data/bronze_trips").history().select(
    "version", "operation"
).show(truncate=False)

spark.stop()

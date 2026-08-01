# local_delta_demo.py — enable Delta Lake on plain OSS Spark and prove it works
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# On Databricks, Delta is the default. On OSS Spark it is NOT — we must wire it in.
# 1) register Delta's SQL extension and catalog
builder = (
    SparkSession.builder
        .appName("phase3-local-delta")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

# 2) this helper downloads the matching delta-spark JARs on first run
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# write a tiny Delta table to local disk (a folder, not a cloud table)
df = spark.range(10)
df.write.format("delta").mode("overwrite").save("data/local_delta_table")

# read it back and show the Delta history — the transaction log works locally too
print("=== data ===")
spark.read.format("delta").load("data/local_delta_table").show()

print("=== history ===")
from delta.tables import DeltaTable
DeltaTable.forPath(spark, "data/local_delta_table").history().select(
    "version", "operation"
).show(truncate=False)

spark.stop()

# check_env.py — verify local Spark works before building the Phase 3 pipeline
from pyspark.sql import SparkSession

# Create a LOCAL Spark session.
# master "local[*]" means: run Spark on THIS machine, using all CPU cores as workers.
spark = (
    SparkSession.builder
        .appName("phase3-env-check")
        .master("local[*]")
        .getOrCreate()
)

# quiet down the log spam
spark.sparkContext.setLogLevel("WARN")

# tiny DataFrame to confirm the engine actually runs
spark.range(5).show()

print("Spark version:", spark.version)
spark.stop()

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("iceberg-smoke-test")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "./warehouse")
    .getOrCreate()
)

spark.sql("CREATE TABLE IF NOT EXISTS local.test_db.hello (id INT, msg STRING) USING iceberg")
spark.sql("INSERT INTO local.test_db.hello VALUES (1, 'it works'), (2, 'iceberg is up')")
spark.sql("SELECT * FROM local.test_db.hello").show()

spark.stop()
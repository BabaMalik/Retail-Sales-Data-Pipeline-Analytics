import shutil
import subprocess


def build_spark_session(app_name: str = "RetailSalesPipeline"):
    java_binary = shutil.which("java")
    if java_binary is None:
        raise RuntimeError(
            "Java runtime not found. Install Java 11+ and set JAVA_HOME before running the retail pipeline."
        )

    java_check = subprocess.run(
        [java_binary, "-version"],
        capture_output=True,
        text=True,
    )
    if java_check.returncode != 0:
        raise RuntimeError(
            "Java runtime is not usable. Install Java 11+ and set JAVA_HOME before running the retail pipeline."
        )

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

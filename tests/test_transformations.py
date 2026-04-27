import shutil
import subprocess
import pytest


pyspark = pytest.importorskip("pyspark")
java_binary = shutil.which("java")
java_ready = False

if java_binary:
    java_check = subprocess.run(
        [java_binary, "-version"],
        capture_output=True,
        text=True,
    )
    java_ready = java_check.returncode == 0

pytestmark = pytest.mark.skipif(
    not java_ready,
    reason="Java runtime is required for Spark-based tests.",
)

from retail_pipeline.transformations import clean_products  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("retail-pipeline-tests")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_clean_products_removes_duplicates_and_standardizes_text(spark):
    input_df = spark.createDataFrame(
        [
            ("P001", "  wireless mouse  ", " electronics ", "computer accessories", 8.0, 15.5),
            ("P001", "wireless mouse", "ELECTRONICS", "COMPUTER ACCESSORIES", 8.0, 15.5),
            ("P002", "Yoga Mat", None, None, 10.0, 25.0),
        ],
        ["product_id", "product_name", "category", "subcategory", "unit_cost", "unit_price"],
    )

    cleaned = clean_products(input_df).orderBy("product_id").collect()

    assert len(cleaned) == 2
    assert cleaned[0]["product_name"] == "Wireless Mouse"
    assert cleaned[0]["category"] == "Electronics"
    assert cleaned[1]["category"] == "Unknown"
    assert cleaned[1]["subcategory"] == "Unknown"

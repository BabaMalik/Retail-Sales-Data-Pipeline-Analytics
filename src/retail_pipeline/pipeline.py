from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retail_pipeline.config import PipelinePaths
from retail_pipeline.schemas import (
    CUSTOMERS_SCHEMA,
    PRODUCTS_SCHEMA,
    STORES_SCHEMA,
    TRANSACTIONS_SCHEMA,
)
from retail_pipeline.spark_utils import build_spark_session
from retail_pipeline.transformations import (
    build_customer_behavior_summary,
    build_daily_sales_summary,
    build_monthly_revenue_summary,
    build_sales_enriched,
    build_store_sales_summary,
    build_top_selling_products,
    clean_customers,
    clean_products,
    clean_stores,
    clean_transactions,
)


def read_csv(spark, path: Path, schema) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(str(path))
    )


def write_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )


def read_delta(spark, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def ingest_bronze_layer(spark, paths: PipelinePaths) -> None:
    sources = {
        "products": (paths.raw_dir / "products.csv", PRODUCTS_SCHEMA),
        "customers": (paths.raw_dir / "customers.csv", CUSTOMERS_SCHEMA),
        "stores": (paths.raw_dir / "stores.csv", STORES_SCHEMA),
        "transactions": (paths.raw_dir / "transactions.csv", TRANSACTIONS_SCHEMA),
    }

    for table_name, (file_path, schema) in sources.items():
        df = read_csv(spark, file_path, schema).withColumn(
            "ingestion_timestamp",
            F.current_timestamp(),
        )
        write_delta(df, paths.bronze_table_path(table_name))


def build_silver_layer(spark, paths: PipelinePaths) -> None:
    bronze_products = read_delta(spark, paths.bronze_table_path("products"))
    bronze_customers = read_delta(spark, paths.bronze_table_path("customers"))
    bronze_stores = read_delta(spark, paths.bronze_table_path("stores"))
    bronze_transactions = read_delta(spark, paths.bronze_table_path("transactions"))

    silver_products = clean_products(bronze_products)
    silver_customers = clean_customers(bronze_customers)
    silver_stores = clean_stores(bronze_stores)
    silver_transactions = clean_transactions(bronze_transactions)

    write_delta(silver_products, paths.silver_table_path("products"))
    write_delta(silver_customers, paths.silver_table_path("customers"))
    write_delta(silver_stores, paths.silver_table_path("stores"))
    write_delta(silver_transactions, paths.silver_table_path("transactions"))


def build_gold_layer(spark, paths: PipelinePaths) -> None:
    silver_products = read_delta(spark, paths.silver_table_path("products"))
    silver_customers = read_delta(spark, paths.silver_table_path("customers"))
    silver_stores = read_delta(spark, paths.silver_table_path("stores"))
    silver_transactions = read_delta(spark, paths.silver_table_path("transactions"))

    sales_enriched = build_sales_enriched(
        transactions_df=silver_transactions,
        products_df=silver_products,
        customers_df=silver_customers,
        stores_df=silver_stores,
    )
    daily_sales_summary = build_daily_sales_summary(sales_enriched)
    monthly_revenue_summary = build_monthly_revenue_summary(sales_enriched)
    top_selling_products = build_top_selling_products(sales_enriched)
    customer_behavior_summary = build_customer_behavior_summary(sales_enriched)
    store_sales_summary = build_store_sales_summary(sales_enriched)

    write_delta(sales_enriched, paths.gold_table_path("sales_enriched"))
    write_delta(daily_sales_summary, paths.gold_table_path("daily_sales_summary"))
    write_delta(
        monthly_revenue_summary,
        paths.gold_table_path("monthly_revenue_summary"),
    )
    write_delta(top_selling_products, paths.gold_table_path("top_selling_products"))
    write_delta(
        customer_behavior_summary,
        paths.gold_table_path("customer_behavior_summary"),
    )
    write_delta(store_sales_summary, paths.gold_table_path("store_sales_summary"))


def print_pipeline_summary(spark, paths: PipelinePaths) -> None:
    summary_tables = [
        "daily_sales_summary",
        "monthly_revenue_summary",
        "top_selling_products",
        "customer_behavior_summary",
        "store_sales_summary",
    ]
    print("\nRetail pipeline completed successfully.\n")
    for table_name in summary_tables:
        df = read_delta(spark, paths.gold_table_path(table_name))
        print(f"Preview: {table_name}")
        df.show(10, truncate=False)


def run_pipeline(project_root: Path) -> None:
    paths = PipelinePaths(project_root=project_root)
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session()
    try:
        ingest_bronze_layer(spark, paths)
        build_silver_layer(spark, paths)
        build_gold_layer(spark, paths)
        print_pipeline_summary(spark, paths)
    finally:
        spark.stop()


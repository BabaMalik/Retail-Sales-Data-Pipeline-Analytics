from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def normalize_whitespace(column_name: str):
    return F.regexp_replace(F.trim(F.col(column_name)), r"\s+", " ")


def titlecase_text(column_name: str):
    return F.initcap(F.lower(normalize_whitespace(column_name)))


def uppercase_text(column_name: str):
    return F.upper(normalize_whitespace(column_name))


def clean_products(products_df: DataFrame) -> DataFrame:
    return (
        products_df.dropDuplicates(["product_id"])
        .filter(F.col("product_id").isNotNull())
        .withColumn("product_name", titlecase_text("product_name"))
        .withColumn(
            "category",
            F.when(F.col("category").isNull(), F.lit("Unknown")).otherwise(
                titlecase_text("category")
            ),
        )
        .withColumn(
            "subcategory",
            F.when(F.col("subcategory").isNull(), F.lit("Unknown")).otherwise(
                titlecase_text("subcategory")
            ),
        )
        .withColumn(
            "unit_cost",
            F.round(F.coalesce(F.col("unit_cost"), F.lit(0.0)), 2),
        )
        .withColumn(
            "unit_price",
            F.round(F.coalesce(F.col("unit_price"), F.lit(0.0)), 2),
        )
        .filter(F.col("unit_price") > 0)
    )


def clean_customers(customers_df: DataFrame) -> DataFrame:
    avg_age_row = customers_df.select(F.round(F.avg("age")).alias("avg_age")).first()
    avg_age = avg_age_row["avg_age"] if avg_age_row and avg_age_row["avg_age"] else 35

    return (
        customers_df.dropDuplicates(["customer_id"])
        .filter(F.col("customer_id").isNotNull())
        .withColumn("first_name", titlecase_text("first_name"))
        .withColumn("last_name", titlecase_text("last_name"))
        .withColumn(
            "gender",
            F.when(F.col("gender").isNull(), F.lit("Unknown")).otherwise(
                titlecase_text("gender")
            ),
        )
        .withColumn("age", F.coalesce(F.col("age"), F.lit(avg_age)))
        .withColumn(
            "city",
            F.when(F.col("city").isNull(), F.lit("Unknown")).otherwise(
                titlecase_text("city")
            ),
        )
        .withColumn(
            "state",
            F.when(F.col("state").isNull(), F.lit("NA")).otherwise(
                uppercase_text("state")
            ),
        )
        .withColumn("signup_date", F.to_date("signup_date"))
    )


def clean_stores(stores_df: DataFrame) -> DataFrame:
    return (
        stores_df.dropDuplicates(["store_id"])
        .filter(F.col("store_id").isNotNull())
        .withColumn("store_name", titlecase_text("store_name"))
        .withColumn("store_type", titlecase_text("store_type"))
        .withColumn("city", titlecase_text("city"))
        .withColumn("state", uppercase_text("state"))
        .withColumn(
            "region",
            F.when(F.col("region").isNull(), F.lit("Unknown")).otherwise(
                titlecase_text("region")
            ),
        )
        .withColumn("open_date", F.to_date("open_date"))
    )


def clean_transactions(transactions_df: DataFrame) -> DataFrame:
    cleaned = (
        transactions_df.dropDuplicates(["transaction_id"])
        .filter(
            F.col("transaction_id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("store_id").isNotNull()
            & F.col("product_id").isNotNull()
        )
        .withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
        .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(1)))
        .withColumn("unit_price", F.coalesce(F.col("unit_price"), F.lit(0.0)))
        .withColumn("discount_pct", F.coalesce(F.col("discount_pct"), F.lit(0.0)))
        .withColumn("payment_method", titlecase_text("payment_method"))
        .withColumn("sales_channel", titlecase_text("sales_channel"))
        .filter(
            F.col("transaction_ts").isNotNull()
            & (F.col("quantity") > 0)
            & (F.col("unit_price") > 0)
        )
    )

    return (
        cleaned.withColumn(
            "total_amount",
            F.when(
                F.col("total_amount").isNull() | (F.col("total_amount") <= 0),
                F.round(
                    F.col("quantity")
                    * F.col("unit_price")
                    * (F.lit(1) - F.col("discount_pct")),
                    2,
                ),
            ).otherwise(F.round(F.col("total_amount"), 2)),
        )
        .withColumn("transaction_date", F.to_date("transaction_ts"))
        .withColumn("year_month", F.date_format("transaction_ts", "yyyy-MM"))
    )


def build_sales_enriched(
    transactions_df: DataFrame,
    products_df: DataFrame,
    customers_df: DataFrame,
    stores_df: DataFrame,
) -> DataFrame:
    return (
        transactions_df.alias("t")
        .join(products_df.alias("p"), on="product_id", how="left")
        .join(customers_df.alias("c"), on="customer_id", how="left")
        .join(stores_df.alias("s"), on="store_id", how="left")
        .select(
            "transaction_id",
            "transaction_ts",
            "transaction_date",
            "year_month",
            "customer_id",
            F.concat_ws(" ", F.col("c.first_name"), F.col("c.last_name")).alias(
                "customer_name"
            ),
            "gender",
            "age",
            F.col("c.city").alias("customer_city"),
            F.col("c.state").alias("customer_state"),
            "store_id",
            F.col("s.store_name").alias("store_name"),
            F.col("s.store_type").alias("store_type"),
            F.col("s.city").alias("store_city"),
            F.col("s.state").alias("store_state"),
            F.col("s.region").alias("region"),
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "quantity",
            "unit_cost",
            "unit_price",
            "discount_pct",
            "total_amount",
            "payment_method",
            "sales_channel",
        )
    )


def build_daily_sales_summary(sales_enriched_df: DataFrame) -> DataFrame:
    return (
        sales_enriched_df.groupBy("transaction_date")
        .agg(
            F.countDistinct("transaction_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("quantity").alias("items_sold"),
            F.round(F.sum("total_amount"), 2).alias("daily_revenue"),
            F.round(F.avg("total_amount"), 2).alias("average_order_value"),
        )
        .orderBy("transaction_date")
    )


def build_monthly_revenue_summary(sales_enriched_df: DataFrame) -> DataFrame:
    monthly_sales = (
        sales_enriched_df.groupBy("year_month")
        .agg(F.round(F.sum("total_amount"), 2).alias("monthly_revenue"))
        .orderBy("year_month")
    )

    growth_window = Window.orderBy("year_month")

    return (
        monthly_sales.withColumn(
            "previous_month_revenue",
            F.lag("monthly_revenue").over(growth_window),
        )
        .withColumn(
            "revenue_growth_pct",
            F.round(
                (
                    (F.col("monthly_revenue") - F.col("previous_month_revenue"))
                    / F.col("previous_month_revenue")
                )
                * 100,
                2,
            ),
        )
    )


def build_top_selling_products(sales_enriched_df: DataFrame) -> DataFrame:
    product_window = Window.orderBy(F.desc("total_revenue"))

    product_sales = (
        sales_enriched_df.groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.countDistinct("transaction_id").alias("orders_count"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    return product_sales.withColumn(
        "sales_rank",
        F.dense_rank().over(product_window),
    )


def build_customer_behavior_summary(sales_enriched_df: DataFrame) -> DataFrame:
    return (
        sales_enriched_df.groupBy("customer_id", "customer_name", "gender", "age")
        .agg(
            F.countDistinct("transaction_id").alias("orders_count"),
            F.round(F.sum("total_amount"), 2).alias("total_spent"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.max("transaction_date").alias("last_purchase_date"),
            F.countDistinct("product_id").alias("unique_products_purchased"),
        )
        .orderBy(F.desc("total_spent"))
    )


def build_store_sales_summary(sales_enriched_df: DataFrame) -> DataFrame:
    return (
        sales_enriched_df.groupBy(
            "store_id",
            "store_name",
            "store_type",
            "region",
            "store_city",
            "store_state",
        )
        .agg(
            F.countDistinct("transaction_id").alias("orders_count"),
            F.round(F.sum("total_amount"), 2).alias("total_sales"),
            F.sum("quantity").alias("items_sold"),
            F.countDistinct("customer_id").alias("customer_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        )
        .orderBy(F.desc("total_sales"))
    )


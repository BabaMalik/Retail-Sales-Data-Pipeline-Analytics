from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("unit_cost", DoubleType(), True),
        StructField("unit_price", DoubleType(), True),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("signup_date", StringType(), True),
    ]
)

STORES_SCHEMA = StructType(
    [
        StructField("store_id", StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("store_type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("region", StringType(), True),
        StructField("open_date", StringType(), True),
    ]
)

TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("transaction_ts", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("sales_channel", StringType(), True),
    ]
)


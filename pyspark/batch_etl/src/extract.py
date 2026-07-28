from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)

# Schema matches the raw CSV header exactly (see data/raw/superstore_sales.csv).
# Order Date / Ship Date are read as strings here because the source format
# (M/d/yyyy) needs an explicit to_date() conversion - that happens in transform.py,
# keeping "parsing untyped data" and "converting to proper types" as separate steps.
RAW_SCHEMA = StructType(
    [
        StructField("Row ID", IntegerType(), True),
        StructField("Order ID", IntegerType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Order Priority", StringType(), True),
        StructField("Order Quantity", IntegerType(), True),
        StructField("Sales", DoubleType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("Ship Mode", StringType(), True),
        StructField("Profit", DoubleType(), True),
        StructField("Unit Price", DoubleType(), True),
        StructField("Shipping Cost", DoubleType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("Province", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("Customer Segment", StringType(), True),
        StructField("Product Category", StringType(), True),
        StructField("Product Sub-Category", StringType(), True),
        StructField("Product Name", StringType(), True),
        StructField("Product Container", StringType(), True),
        StructField("Product Base Margin", DoubleType(), True),
        StructField("Ship Date", StringType(), True),
    ]
)

def extract_sales_data(spark: SparkSession, input_path : str) -> DataFrame:


    df = spark.read.option('header',True).option('quotes','"').option('escape','"').schema(RAW_SCHEMA).csv(input_path)
    return df
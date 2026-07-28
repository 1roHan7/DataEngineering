"""
clean_sales_data() - fix column names, parse dates, drop bad rows.
add_derived_columns - compute new bussiness metrics
aggregate_* - build the summary tables
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_sales_data(df: DataFrame)-> DataFrame:
    renamed = (
        df.withColumnRenamed("Row ID", "row_id")
        .withColumnRenamed("Order ID", "order_id")
        .withColumnRenamed("Order Date", "order_date_raw")
        .withColumnRenamed("Order Priority", "order_priority")
        .withColumnRenamed("Order Quantity", "order_quantity")
        .withColumnRenamed("Sales", "sales")
        .withColumnRenamed("Discount", "discount")
        .withColumnRenamed("Ship Mode", "ship_mode")
        .withColumnRenamed("Profit", "profit")
        .withColumnRenamed("Unit Price", "unit_price")
        .withColumnRenamed("Shipping Cost", "shipping_cost")
        .withColumnRenamed("Customer Name", "customer_name")
        .withColumnRenamed("Province", "province")
        .withColumnRenamed("Region", "region")
        .withColumnRenamed("Customer Segment", "customer_segment")
        .withColumnRenamed("Product Category", "product_category")
        .withColumnRenamed("Product Sub-Category", "product_subcategory")
        .withColumnRenamed("Product Name", "product_name")
        .withColumnRenamed("Product Container", "product_container")
        .withColumnRenamed("Product Base Margin", "product_base_margin")
        .withColumnRenamed("Ship Date", "ship_date_raw")
    )


    cleaned = (
        renamed.withColumn(
            'order_date',F.to_date('order_date_raw','M/d/yyyy')
        )
        .withColumn('ship_date', F.to_date('ship_date_raw','M/d/yyyy'))
        .drop('ship_date_raw','order_date_raw')
        .filter(F.col('row_id').isNotNull())
        .fillna({'product_base_margin': 0.0})
    )
    return cleaned

def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add business-friendly derived columns:
        - profit_margin_pct: profit as a % of sales (rounded to 2 decimals)
        - order_year / order_month: extracted for easy grouping/reporting
        - is_profitable: simple boolean flag, handy for quick filtering
    """ 
    return(
        df.withColumn(
            'profit_margin_pct',
            F.round((F.col('profit')/F.col('sales'))*100, 2)
        )
        .withColumn('order_year', F.year('order_date'))
        .withColumn('order_month', F.month('order_date'))
        .withColumn('is_profitable', F.col('profit')> 0)

    )
def aggregate_revenue_by_region_month(df : DataFrame) -> DataFrame:

    return(
        df.groupBy("region", "order_year", "order_month")
        .agg(
            F.round(F.sum('sales'),2).alias('total_sales'),
            F.round(F.sum('profit'),2).alias('total_profit'),
            F.round(F.sum('order_id'),2).alias('order_count'),
        )
    )



from pyspark.sql import DataFrame


def write_table(df: DataFrame, output_dir: str, table_name: str) -> None:
    df.write.mode("overwrite").parquet(f"{output_dir}/{table_name}.parquet")
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/{table_name}_csv")
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform sales CSV with total_amount.")
    parser.add_argument("--input", required=True, help="Input CSV path in GCS.")
    parser.add_argument("--output", required=True, help="Output CSV directory path in GCS.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("sales-transform").getOrCreate()

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(args.input)
    )

    transformed = (
        df.withColumn("quantity", col("quantity").cast("int"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("total_amount", col("quantity") * col("price"))
        .select(
            "order_id",
            "customer_name",
            "country",
            "product",
            "quantity",
            "price",
            "total_amount",
        )
    )

    transformed.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output)
    spark.stop()


if __name__ == "__main__":
    main()

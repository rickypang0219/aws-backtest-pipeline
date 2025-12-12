import sys

from awsglue.transforms import *  # pyright: ignore [reportMissingImports] # noqa: F403
from awsglue.utils import getResolvedOptions  # pyright: ignore [reportMissingImports]
from awsglue.context import GlueContext  # pyright: ignore [reportMissingImports]
from awsglue.job import Job  # pyright: ignore [reportMissingImports]


from pyspark.context import SparkContext  # pyright: ignore [reportMissingImports]
from pyspark.sql import DataFrame  # pyright: ignore [reportMissingImports]
from pyspark.sql.functions import (  # pyright: ignore [reportMissingImports]
    col,
    sum as pyspark_sum,
    window,
    lit,
    coalesce,
    when,
)
from pyspark.sql.types import LongType, DoubleType, BooleanType, TimestampType  # pyright: ignore [reportMissingImports]


def lazy_resample_df_pyspark(df: DataFrame) -> DataFrame:
    df_casted = (
        df.withColumn("time", col("time").cast(LongType()))
        .withColumn("qty", col("qty").cast(DoubleType()))
        .withColumn("quote_qty", col("quote_qty").cast(DoubleType()))
        .withColumn("is_buyer_maker", col("is_buyer_maker").cast(BooleanType()))
        .withColumn("time", col("time").cast(LongType()))
    )
    df_with_time_dt = df_casted.withColumn(
        "time_dt", (col("time") / 1000).cast(TimestampType())
    )

    resampled_df = df_with_time_dt.groupBy(
        window(col("time_dt"), "1 minutes", "1 minutes", "0 seconds")
    ).agg(
        coalesce(pyspark_sum(col("qty")), lit(0)).alias("total_volume"),
        coalesce(pyspark_sum(col("quote_qty")), lit(0)).alias("turnover"),
        coalesce(pyspark_sum(when(~col("is_buyer_maker"), col("qty"))), lit(0)).alias(
            "buy_volume"
        ),
        coalesce(pyspark_sum(when(col("is_buyer_maker"), col("qty"))), lit(0)).alias(
            "sell_volume"
        ),
        coalesce(
            pyspark_sum(when(~col("is_buyer_maker"), col("quote_qty"))), lit(0)
        ).alias("buy_quote_volume"),
        coalesce(
            pyspark_sum(when(col("is_buyer_maker"), col("quote_qty"))), lit(0)
        ).alias("sell_quote_volume"),
    )
    return resampled_df.select(
        col("window"),
        col("total_volume"),
        col("turnover"),
        col("buy_volume"),
        col("sell_volume"),
        (col("buy_volume") - col("sell_volume")).alias("qty_flow"),
        (col("buy_quote_volume") - col("sell_quote_volume")).alias("quote_qty_flow"),
    ).orderBy("window.start")


args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "S3_TARGET_PATH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_SOURCE_PATH = args["S3_SOURCE_PATH"]
S3_TARGET_PATH = args["S3_TARGET_PATH"]

print(f"Reading zipped CSVs from: {S3_SOURCE_PATH}")
print(f"Writing Parquet to: {S3_TARGET_PATH}")


try:
    raw_df = spark.read.format("parquet").load(S3_SOURCE_PATH)

    print("Schema of the unzipped CSV data:")
    raw_df.printSchema()

    print("Applying custom transformations...")
    transformed_df = lazy_resample_df_pyspark(raw_df)

    num_output_partitions = (
        transformed_df.rdd.getNumPartitions()
    )  # Start with current partitions
    if num_output_partitions > 48:  # Cap partitions to a reasonable number for output
        num_output_partitions = 48
    elif num_output_partitions < 12:  # Ensure a minimum number of partitions
        num_output_partitions = 12

    print(f"Repartitioning data to {num_output_partitions} for writing to Parquet...")
    output_df = transformed_df.repartition(num_output_partitions)

    print(f"Writing transformed data to Parquet at: {S3_TARGET_PATH}")
    output_df.write.mode("overwrite").parquet(S3_TARGET_PATH)

    print("ETL job completed successfully!")

except Exception as e:
    print(f"An error occurred during ETL job execution: {e}")
    raise e

job.commit()

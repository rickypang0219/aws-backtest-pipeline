import polars as pl

# file_name = "test_concat_data_duckdb_limited.parquet"
file_name = "liq_heatmap_3d.parquet"


# json_file_name = "response_1761136505077.json"
json_file_name = "response_1761292702303.json"

df = (
    pl.scan_parquet(file_name)
    .with_columns(
        pl.col("endingPrice").cast(pl.Int64),
        pl.col("startingPrice").cast(pl.Int64),
    )
    .filter((pl.col("timestamp") == 1739268900) & (pl.col("startingPrice") == 114950))
    .sort(
        by=["timestamp", "startingPrice", "endingPrice"], descending=[False, True, True]
    )
    .select(["timestamp", "size", "startingPrice", "endingPrice", "side"])
    .collect(engine="streaming")
)


print(df)


# df_json = pl.read_json(json_file_name).explode("data").unnest("data").drop("metadata")
#
# print(df_json)
#
#
# df_diff = df.join(
#     df_json,
#     on=["timestamp", "size", "startingPrice", "endingPrice", "side"],
#     how="full",
#     suffix="_df2",
# )
# print(df_diff)
#
#
# print(df_diff.filter(pl.col("size").is_null() | pl.col("size_df2").is_null()))

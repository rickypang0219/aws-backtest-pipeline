from pathlib import Path
import duckdb

input_folder = "hyblock_liq_heatmap/BTC/binance_perp_stable/lookback=3d/scaling=relative/leverage=all"
parquet_files = Path(input_folder) / "*.parquet"
output_file = "test_concat_data_duckdb_limited.parquet"

con = duckdb.connect(database=":memory:", read_only=False)

con.execute("SET memory_limit='8GB';")
con.execute("INSTALL parquet; LOAD parquet;")

# query = f"""
# COPY (
#     SELECT DISTINCT
#         * -- Select all columns, but only unique rows
#     FROM
#         read_parquet('{parquet_files}', union_by_name=true)
#     ORDER BY
#         timestamp ASC
# )
# TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
# """


query = f"""
COPY (
        SELECT DISTINCT
        timestamp,
        size,
        endingPrice,
        startingPrice,
        side
    FROM
        read_parquet('{parquet_files}', union_by_name=true)
    ORDER BY
        timestamp ASC,
        endingPrice DESC,
        startingPrice DESC
)
TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
"""

con.execute(query)
con.close()
print(
    f"DuckDB sorting complete (forced external sort via memory limit) to {output_file}"
)

if __name__ == "__main__":
    import polars as pl

    output_file = "test_concat_data_duckdb_limited.parquet"
    print(pl.scan_parquet(output_file).collect(engine="streaming"))

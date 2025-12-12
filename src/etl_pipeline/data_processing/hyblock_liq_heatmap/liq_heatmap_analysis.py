import polars as pl


file_name = "test_concat_data_duckdb_limited.parquet"
file_name = "liq_heatmap_3d.parquet"


ldf = pl.scan_parquet(file_name)

ldf = ldf.with_columns(
    pl.col("endingPrice").cast(pl.Int64),
    pl.col("startingPrice").cast(pl.Int64),
    pl.from_epoch("timestamp", time_unit="s").alias("time_dt"),
).with_columns(pl.col("time_dt").dt.truncate("1h").alias("time_bin"))

ldf = (
    ldf.group_by(["time_bin", "side"])
    .agg(
        total_size=pl.col("size").sum(),
        avg_size=pl.col("size").mean(),
    )
    .sort(["time_bin", "side"])
)

df = ldf.collect(engine="streaming")

if __name__ == "__main__":
    import numpy as np
    import matplotlib.pyplot as plt

    df_long = df.filter(pl.col("side") == "long")
    df_short = df.filter(pl.col("side") == "short")
    # plt.plot(df_long["time_bin"], df_long["total_size"], label="long")
    #
    # plt.plot(df_short["time_bin"], df_short["total_size"], label="short")

    diff = (df_long["total_size"] - df_short["total_size"]).to_numpy()
    normalize_diff = np.zeros_like(diff)
    diff_mean = diff.mean()
    diff_std = diff.std()
    np.divide(
        diff - diff_mean,
        diff_std,
        out=normalize_diff,
        where=(diff_std != 0),
    )

    plt.plot(
        df_short["time_bin"],
        normalize_diff,
        # df_long["total_size"] - df_short["total_size"],
        label="diff",
    )
    plt.legend()
    plt.show()

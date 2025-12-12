import polars as pl
import pandas as pd

# --- 1. Define the Data ---
data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
window_size = 4
quantile_val = 0.25

# --- 2. Pandas Implementation (Using Defaults) ---
df_pd = pd.DataFrame({"A": data})

# 1. No min_periods specified (defaults to window_size=4)
# 2. No closed specified (defaults to 'right')
df_pd_result = df_pd.assign(
    rolling_quantile_pd=df_pd["A"].rolling(window=window_size).quantile(quantile_val)
)

# --- 3. Polars Implementation (Matching Pandas Defaults) ---
df_pl = pl.DataFrame({"A": data})

# 1. No min_samples specified (Polars default is window_size=4)
# 2. Polars' rolling_quantile defaults to a right-aligned window
df_pl_result = df_pl.with_columns(
    rolling_quantile_pl=pl.col("A").rolling_quantile(
        quantile=quantile_val,
        window_size=window_size,
        interpolation="linear",
        # OMITTING 'min_samples' here makes it default to 'window_size',
        # matching Pandas' default for 'min_periods'.
    ),
).select(["A", "rolling_quantile_pl"])


# --- 4. Comparison ---

# Combine the results for a side-by-side view
comparison_df = df_pl_result.with_columns(
    pl.Series("rolling_quantile_pd", df_pd_result["rolling_quantile_pd"].tolist())
)

print(
    f"Comparison (q={quantile_val}, window={window_size}, default min_periods/min_samples)"
)
print("-" * 75)
print(comparison_df)
print("-" * 75)
print("Note: The first three results are null (NaN) in both frameworks because")
print("the minimum required samples/periods defaulted to the window size (4).")

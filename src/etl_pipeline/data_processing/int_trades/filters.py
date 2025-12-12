import polars as pl

price_is_int_expr = (
    pl.col("price")
    - (pl.col("price").truediv(10 * pl.col("tick_size"))).round()
    * (10 * pl.col("tick_size"))
).abs() <= 1e-8


price_is_int10_expr = (
    pl.col("price")
    - (pl.col("price").truediv(100 * pl.col("tick_size"))).round()
    * (100 * pl.col("tick_size"))
).abs() <= 1e-8


qty_dec_2_scaled_expr = (
    (pl.col("qty") * pl.col("dec_2_scale")).round().truediv(pl.col("dec_2_scale"))
    - pl.col("qty")
).abs().round(8) == 0

qty_dec_1_scaled_expr = (
    (pl.col("qty") * pl.col("dec_1_scale")).round().truediv(pl.col("dec_1_scale"))
    - pl.col("qty")
).abs().round(8) == 0

qty_is_int_scaled_expr = (
    (pl.col("qty") * pl.col("int_scale")).round().truediv(pl.col("int_scale"))
    - pl.col("qty")
).abs().round(8) == 0

quote_qty_is_int_scaled_expr = pl.col("quote_qty").round() == pl.col("quote_qty")
quote_qty_is_int10_scaled_expr = (
    pl.col("quote_qty") == (pl.col("quote_qty") / 10).round() * 10
)
quote_qty_is_int100_scaled_expr = (
    pl.col("quote_qty") == (pl.col("quote_qty") / 100).round() * 100
)
quote_qty_is_int1000_scaled_expr = (
    pl.col("quote_qty") == (pl.col("quote_qty") / 1000).round() * 1000
)


int_price_qty_filters = {
    "int_price_qty": (price_is_int_expr | price_is_int10_expr)
    & (qty_is_int_scaled_expr | qty_dec_1_scaled_expr | qty_dec_2_scaled_expr),
    "int_price_int_qty": (price_is_int_expr) & qty_is_int_scaled_expr,
    "int_price_dec_1_qty": (price_is_int_expr) & qty_dec_1_scaled_expr,
    "int_price_dec_2_qty": price_is_int_expr & qty_dec_2_scaled_expr,
    "int10_price_int_qty": price_is_int10_expr & qty_is_int_scaled_expr,
    "int10_price_dec_1_qty": price_is_int10_expr & qty_dec_1_scaled_expr,
    "int10_price_dec_2_qty": price_is_int10_expr & qty_dec_2_scaled_expr,
}


int_price_quote_qty_filters = {
    "int_price_quote_qty": (price_is_int_expr | price_is_int10_expr)
    & (
        quote_qty_is_int_scaled_expr
        | quote_qty_is_int_scaled_expr
        | quote_qty_is_int100_scaled_expr
        | quote_qty_is_int1000_scaled_expr
    ),
    "int_price_int_quote_qty": (price_is_int_expr) & quote_qty_is_int_scaled_expr,
    "int_price_int10_quote_qty": (price_is_int_expr) & quote_qty_is_int10_scaled_expr,
    "int_price_int100_quote_qty": price_is_int_expr & quote_qty_is_int100_scaled_expr,
    "int_price_int1000_quote_qty": price_is_int_expr & quote_qty_is_int1000_scaled_expr,
    "int10_price_int_quote_qty": (price_is_int10_expr) & quote_qty_is_int_scaled_expr,
    "int10_price_int10_quote_qty": (price_is_int10_expr)
    & quote_qty_is_int10_scaled_expr,
    "int10_price_int100_quote_qty": price_is_int10_expr
    & quote_qty_is_int100_scaled_expr,
    "int10_price_int1000_quote_qty": price_is_int10_expr
    & quote_qty_is_int1000_scaled_expr,
}

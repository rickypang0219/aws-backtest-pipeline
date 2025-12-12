import zipfile
import polars as pl


from pathlib import Path
from polars.exceptions import ComputeError
from data_processing.int_trades.shared import logger


CSV_SCHEMA = {
    "id": pl.Int64,
    "price": pl.Float64,
    "qty": pl.Float64,
    "quote_qty": pl.Float64,
    "time": pl.Int64,
    "is_buyer_maker": pl.Boolean,
}

EXPECTED_HEADER = ",".join(CSV_SCHEMA.keys())


def _add_largest_decimal_column(
    lazy_df: pl.LazyFrame, price_col: str = "price"
) -> pl.LazyFrame:
    decimal_counts = lazy_df.select(
        pl.col(price_col)
        .cast(pl.Utf8)
        .str.split(".")
        .list.get(1, null_on_oob=True)
        .str.strip_chars_end("0")
        .str.len_chars()
        .fill_null(0)
        .alias("decimal_count")
    )
    max_decimals = decimal_counts.select(pl.col("decimal_count").max()).collect().item()
    return lazy_df.with_columns(pl.lit(max_decimals).alias("largest_decimal"))


def _check_csv_has_header(file_path: str) -> bool:
    try:
        with Path(file_path).open(encoding="utf-8") as f:
            first_line = f.readline().strip()
            logger.info("First line of CSV: %s", first_line)
            normalized_first_line = ",".join(
                first_line.lower().replace(" ", "").split(",")
            )
            normalized_expected = EXPECTED_HEADER.lower().replace(" ", "")
            return normalized_first_line == normalized_expected
    except FileNotFoundError as e:
        logger.error("CSV file not found: %s", e)
        return False
    except Exception as e:
        logger.error("Unexpected error checking CSV header: %s", e)
        return False


def _resample_with_filters(
    ldf: pl.LazyFrame,
    filter_expressions: dict[str, pl.Expr],
) -> pl.LazyFrame:
    ldf = ldf.with_columns(pl.from_epoch("time", time_unit="ms").alias("time_dt"))
    ldf = ldf.with_columns(pl.col("time_dt").dt.truncate("1m").alias("time_bin"))

    base_aggregations = [
        pl.col("qty").sum().alias("total_volume").fill_null(0),
        pl.col("price").first().alias("open").fill_null(0),
        pl.col("price").max().alias("high").fill_null(0),
        pl.col("price").min().alias("low").fill_null(0),
        pl.col("price").last().alias("close").fill_null(0),
    ]

    dynamic_factor_aggregations = []
    factor_col_names = []

    for filter_name, filter_expr in filter_expressions.items():
        if not isinstance(filter_expr, pl.Expr):
            raise TypeError(
                f"Filter expression for '{filter_name}' must be a Polars Boolean Expression."
            )
        factor_agg_expr = (
            pl.when(filter_expr)
            .then(
                pl.col("qty") * pl.when(~pl.col("is_buyer_maker")).then(1).otherwise(-1)
            )
            .otherwise(0)
            .sum()
            .alias(f"factor_{filter_name}_flow")
        )
        dynamic_factor_aggregations.append(factor_agg_expr)
        factor_col_names.append(f"factor_{filter_name}_flow")

    all_aggregations = base_aggregations + dynamic_factor_aggregations

    resampled_ldf = ldf.group_by("time_bin").agg(all_aggregations).sort("time_bin")

    columns_to_keep = [
        "time_bin",
        "open",
        "high",
        "low",
        "close",
        "total_volume",
        *factor_col_names,
    ]

    return resampled_ldf.select(columns_to_keep).rename({"time_bin": "time_dt"})


def extract_zip(
    zip_path: str,
    extract_path: str,
) -> None:
    try:
        dir_path = Path(f"/tmp/{extract_path}").parent  # noqa: S108
        tmp_zip_path = f"/tmp/{zip_path}"  # noqa: S108
        tmp_extract_path = f"/tmp/{extract_path}"  # noqa: S108
        dir_path.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(tmp_zip_path, "r") as zip_ref:
            zip_ref.extractall(Path(tmp_extract_path).parent)
        logger.info("ZIP file successfully extracted to %s", extract_path)

    except zipfile.BadZipFile as e:
        logger.error("Error: Invalid or corrupted ZIP file: %s", e)
        raise
    except OSError as e:
        logger.error("Error extracting file to %s:%s ", extract_path, e)
        raise
    except Exception as e:
        logger.error("Unexpected error:%s", e)
        raise


def process_csv_data(  # noqa:  PLR0913
    zip_path: str,
    extract_path: str,
    s3_bucket_name: str,
    filter_exprs: dict[str, pl.Expr],
    tick_size: float,
    price: float,
) -> None:
    temp_zip_path = zip_path
    parquet_s3_key = temp_zip_path.replace(
        "raw_data",
        "resampled_data",
    ).replace(
        "zip",
        "parquet",
    )

    try:
        extract_zip(zip_path, extract_path)  # handle adding /tmp/ inside
        has_header = _check_csv_has_header(str(f"/tmp/{extract_path}"))  # noqa: S108
        logger.info("CSV has header: %s", has_header)

        if has_header:
            ldf = pl.scan_csv(str(f"/tmp/{extract_path}"), has_header=True)  # noqa: S108
        else:
            ldf = pl.scan_csv(
                str(f"/tmp/{extract_path}"),  # noqa: S108
                has_header=False,
                schema=CSV_SCHEMA,
            )

        ldf = ldf.with_columns(pl.lit(tick_size).alias("tick_size"))
        ldf = ldf.with_columns(
            ((pl.lit(1000.0).truediv(pl.lit(price + 1e-8))).log10() - 1)
            .cast(pl.Int64)
            .alias("qty_int_digits"),
        )

        ldf = ldf.with_columns(
            (pl.lit(10.0).pow(-1 * pl.col("qty_int_digits"))).alias("dec_3_scale"),
            (pl.lit(10.0).pow(-1 * (pl.col("qty_int_digits") + 1))).alias(
                "dec_2_scale"
            ),
            (pl.lit(10.0).pow(-1 * (pl.col("qty_int_digits") + 2))).alias(
                "dec_1_scale"
            ),
            (pl.lit(10.0).pow(-1 * (pl.col("qty_int_digits") + 3))).alias("int_scale"),
        )

        _resample_with_filters(ldf, filter_exprs).sink_parquet(
            f"s3://{s3_bucket_name}/{parquet_s3_key}", engine="streaming"
        )
    except zipfile.BadZipFile as e:
        logger.error("Invalid zip file %s", e)
        raise
    except ComputeError as e:
        logger.error("Polars Compute Error %s", e)
        raise
    except Exception as e:
        logger.error("unexpected Error: %s", e)
        raise

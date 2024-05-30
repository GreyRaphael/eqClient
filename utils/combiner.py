import polars as pl
import argparse
import os


def combine_tick(year_month: int, in_dir: str, output_dir: str):
    in_files = f"{in_dir}/{year_month}*/*.parquet"
    print("===>reading", in_files)

    df = (
        pl.scan_parquet(in_files)
        .filter((pl.col("open") != 0) & (pl.col("last") != 0))
        .select(
            pl.col("code").cast(pl.UInt32),
            (pl.col("date").cast(pl.Utf8) + pl.col("time").cast(pl.Utf8).str.pad_start(9, "0")).str.to_datetime("%Y%m%d%H%M%S%3f").alias("dt"),
            pl.col("preclose").cast(pl.UInt32),
            pl.col("open").cast(pl.UInt32),
            pl.col("last").cast(pl.UInt32),
            pl.col("num_trades").cast(pl.UInt32).fill_null(0),
            pl.col("volume").cast(pl.UInt64).fill_null(0),
            pl.col("total_ask_volume").cast(pl.UInt64).fill_null(0).alias("tot_av"),
            pl.col("total_bid_volume").cast(pl.UInt64).fill_null(0).alias("tot_bv"),
            pl.col("amount").cast(pl.UInt64).fill_null(0),
            pl.col("ask_avg_price").cast(pl.UInt32).alias("avg_ap"),
            pl.col("bid_avg_price").cast(pl.UInt32).alias("avg_bp"),
            *[pl.col("ask_prices").list.get(i).cast(pl.UInt32).fill_null(0).alias(f"ap{i}") for i in range(10)],
            *[pl.col("bid_prices").list.get(i).cast(pl.UInt32).fill_null(0).alias(f"bp{i}") for i in range(10)],
            *[pl.col("ask_volumes").list.get(i).cast(pl.UInt32).fill_null(0).alias(f"av{i}") for i in range(10)],
            *[pl.col("bid_volumes").list.get(i).cast(pl.UInt32).fill_null(0).alias(f"bv{i}") for i in range(10)],
        )
        .sort(["code", "dt"])
        .collect()
    )
    print("===>collected")
    null_cont_sum = df.null_count().select("code", "dt", "preclose", "open", "last").sum_horizontal().item(0)
    if null_cont_sum > 0:
        print(df.null_count().select("code", "dt", "preclose", "open", "last", "avg_ap", "avg_bp").to_dicts())

    os.makedirs(output_dir, exist_ok=True)
    out_file = f"{output_dir}/{year_month}.ipc"
    df.write_ipc(out_file, compression="zstd")
    print("===>finish", out_file)


def combine_kl1m(year_month: int, in_dir: str, output_dir: str):
    pass


def gen_ym_list(ym_start: int, ym_end: int) -> list:
    year_start = ym_start // 100
    year_end = ym_end // 100
    ym_list = [year * 100 + month for year in range(year_start, year_end + 1) for month in range(1, 13)]
    return [ym for ym in ym_list if ym_start <= ym <= ym_end]


def process(args):
    in_dir = f"{args.secu_type}/{args.quote_type}"  # etf/tick/
    out_dir = f"{args.secu_type}-{args.quote_type}"  # etf-tick/
    if args.quote_type == "kl1m":
        combiner = combine_kl1m
    else:
        combiner = combine_tick

    for year_month in gen_ym_list(args.ym_start, args.ym_end):
        combiner(year_month, in_dir, out_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="history quotes combiner")
    parser.add_argument("-yms", type=int, required=True, dest="ym_start", help="start year-month, 200701")
    parser.add_argument("-yme", type=int, required=True, dest="ym_end", help="end year-month, 202412")
    parser.add_argument("-st", type=str, required=True, dest="secu_type", choices=["stock", "etf"], help="security type")
    parser.add_argument("-qt", type=str, required=True, dest="quote_type", choices=["tick", "kl1m"], help="quote type")

    args = parser.parse_args()
    process(args)

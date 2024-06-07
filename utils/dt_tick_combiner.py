import polars as pl
import argparse
import json
import os


def combine_tick(target_date: int, in_dir: str, out_dir: str):
    in_files = f"{in_dir}/{target_date}/*.parquet"

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
    null_cont_sum = df.null_count().select("code", "dt", "preclose", "open", "last").sum_horizontal().item(0)
    if null_cont_sum > 0:
        print(df.null_count().select("code", "dt", "preclose", "open", "last", "avg_ap", "avg_bp").to_dicts())

    final_dir = f"{out_dir}/{target_date // 10000}"
    os.makedirs(final_dir, exist_ok=True)
    out_file = f"{final_dir}/{target_date}.ipc"
    df.write_ipc(out_file, compression="zstd")


def gen_dates(yr_start: int, yr_end: int) -> list[int]:
    target_dates = []
    for year in range(yr_start, yr_end + 1):
        with open(f"calendar/{year}.json", "r") as file:
            target_dates += json.load(file)
    return target_dates


def process(args):
    in_dir = f"{args.secu_type}/tick"  # etf/tick/
    out_dir = f"{args.secu_type}-tick"  # etf-tick/

    for target_date in gen_dates(args.yr_start, args.yr_end):
        if not os.path.exists(f"{in_dir}/{target_date}"):
            print("===>not exist", target_date)
            continue

        try:
            combine_tick(target_date, in_dir, out_dir)
        except Exception as e:
            print(f"===>failed of {target_date}", e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="history tick combiner day by day")
    parser.add_argument("-yrs", type=int, required=True, dest="yr_start", help="start year, 2007")
    parser.add_argument("-yre", type=int, required=True, dest="yr_end", help="end year, 2024")
    parser.add_argument("-st", type=str, required=True, dest="secu_type", choices=["stock", "etf"], help="security type")

    args = parser.parse_args()
    process(args)

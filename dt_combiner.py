import polars as pl
import argparse
import json
import os
from utils import chatbot


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
            pl.col("iopv").fill_null(0).cast(pl.UInt32),
            pl.col("high_limit").fill_null(0).cast(pl.UInt32),
            pl.col("low_limit").fill_null(0).cast(pl.UInt32),
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
            *[pl.col("ask_nums").list.get(i).cast(pl.UInt32).fill_null(0).alias(f"an{i}") for i in range(10)],
            *[pl.col("bid_nums").list.get(i).cast(pl.UInt32).fill_null(0).alias(f"bn{i}") for i in range(10)],
        )
        .sort(["code", "dt"])
        .collect()
    )
    null_cont_sum = df.null_count().select("code", "dt", "preclose", "open", "last").sum_horizontal().item(0)
    if null_cont_sum > 0:
        chatbot.send_msg(f'{in_dir}/{target_date} null_count: {df.null_count().select("code", "dt", "preclose", "open", "last", "avg_ap", "avg_bp").to_dicts()}')

    final_dir = f"{out_dir}/{target_date // 10000}"
    os.makedirs(final_dir, exist_ok=True)
    out_file = f"{final_dir}/{target_date}.ipc"
    df.write_ipc(out_file, compression="zstd")


def combine_kl1m(target_date: int, in_dir: str, out_dir: str):
    in_files = f"{in_dir}/{target_date}/*.parquet"

    df = (
        pl.scan_parquet(in_files)
        .with_columns(
            pl.col("open").replace(-1, 0),
            pl.col("high").replace(-1, 0),
            pl.col("low").replace(-1, 0),
            pl.col("last").replace(-1, 0),
        )
        .select(
            pl.col("code").cast(pl.UInt32),
            (pl.col("date").cast(pl.Utf8) + pl.col("time").cast(pl.Utf8).str.pad_start(9, "0")).str.to_datetime("%Y%m%d%H%M%S%3f").alias("dt"),
            pl.col("open").cast(pl.UInt32),
            pl.col("high").cast(pl.UInt32),
            pl.col("low").cast(pl.UInt32),
            pl.col("last").cast(pl.UInt32),
            pl.col("num_trades").cast(pl.UInt32),
            pl.col("volume").cast(pl.UInt64),
            pl.col("amount").cast(pl.UInt64),
        )
        .sort(["code", "dt"])
        .collect()
    )
    null_cont_sum = df.null_count().sum_horizontal().item(0)
    if null_cont_sum > 0:
        chatbot.send_msg(f"{in_dir}/{target_date} null_count: {df.null_count().to_dicts()}")

    final_dir = f"{out_dir}/{target_date // 10000}"
    os.makedirs(final_dir, exist_ok=True)
    out_file = f"{final_dir}/{target_date}.ipc"
    df.write_ipc(out_file, compression="zstd")


def do_comine(secu_type: str, quote_type: str, target_dates: list[int]):
    in_dir = f"{secu_type}/{quote_type}"  # etf/tick/
    out_dir = f"transfer/{secu_type}-{quote_type}"  # transfer/etf-tick/
    if quote_type == "kl1m":
        combiner = combine_kl1m
    else:
        combiner = combine_tick

    for target_date in target_dates:
        if not os.path.exists(f"{in_dir}/{target_date}"):
            chatbot.send_msg(f"{secu_type}:{quote_type}:{target_date} not exist")
            continue

        try:
            combiner(target_date, in_dir, out_dir)
        except Exception as e:
            chatbot.send_msg(f"{secu_type}:{quote_type}:{target_date} failed, {e}")
    chatbot.send_msg(f"{secu_type}:{quote_type}:{target_dates[0]}~{target_dates[-1]} combiner done")


def gen_dt_list(dt_start: int, dt_end: int) -> list[int]:
    yr_start = dt_start // 10000
    yr_end = dt_end // 10000
    year_dates = []
    for year in range(yr_start, yr_end + 1):
        with open(f"calendar/{year}.json", "r") as file:
            year_dates += json.load(file)
    return [target_dt for target_dt in year_dates if dt_start <= target_dt <= dt_end]


def process(args):
    target_dates = gen_dt_list(args.dt_start, args.dt_end)
    do_comine(args.secu_type, args.quote_type, target_dates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="quotes combiner day by day")
    parser.add_argument("-dts", type=int, required=True, dest="dt_start", help="start date, 20070104")
    parser.add_argument("-dte", type=int, required=True, dest="dt_end", help="end date, 20240504")
    parser.add_argument("-st", type=str, required=True, dest="secu_type", choices=["stock", "etf"], help="security type")
    parser.add_argument("-qt", type=str, required=True, dest="quote_type", choices=["tick", "kl1m"], help="quote type")

    args = parser.parse_args()
    process(args)

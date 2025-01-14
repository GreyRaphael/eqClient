import os
import datetime as dt
import argparse
import json
import polars as pl


def gen_bar1m(target_date: int, minute_interval: int, in_dir: str, out_dir: str):
    target_dt = dt.date(target_date // 10000, (target_date // 100) % 100, target_date % 100)
    in_file = target_dt.strftime(f"{in_dir}/%Y/%Y%m%d.ipc")  # etf-tick/2022/20220104.ipc
    df = pl.read_ipc(in_file, memory_map=False)  # read tick, df is sorted by [code, dt]

    # duplicate the first row of each code to 8:00
    df_first = (
        df.group_by("code")
        .first()
        .with_columns(
            pl.col("dt").dt.replace(hour=8),
            volume=pl.lit(0, pl.UInt64),
            amount=pl.lit(0, pl.UInt64),
            num_trades=pl.lit(0, pl.UInt32),
        )
    )

    # change >=11:30:00 to 11:29:59.999
    # change >=15:00:00 to 14:59:59.999
    df_body = df.with_columns(
        pl.when(
            ((pl.col("dt").dt.hour() == 11) & (pl.col("dt").dt.minute() == 30)) | ((pl.col("dt").dt.hour() == 15) & (pl.col("dt").dt.minute() == 0)),
        )
        .then(
            pl.col("dt").dt.replace(second=0, microsecond=0) - dt.timedelta(milliseconds=1),
        )
        .otherwise("dt")
        .alias("dt")
    )

    # concat the two dataframes
    dff = pl.concat([df_first, df_body]).sort(by=["code", "dt"], maintain_order=True)  # maintain_order=True is important for multiple 14:59:59.999
    # group by 1 minute

    df_bar1m = (
        dff.group_by_dynamic("dt", every="1m", period="1m", closed="left", label="right", group_by="code")
        .agg(
            [
                pl.last("preclose").alias("preclose"),
                pl.first("last").alias("open"),
                pl.max("last").alias("high"),
                pl.min("last").alias("low"),
                pl.last("last").alias("close"),
                pl.last("volume"),
                pl.last("amount"),
                pl.last("num_trades"),
            ]
        )
        .sort(by=["code", "dt"])  # maintain_order=True is not necessary as dt will not be the same
        .with_columns(
            pl.col("volume").diff().over("code").cast(pl.UInt64),  # for single target_dt, no need to over("code", pl.col('dt').dt.date())
            pl.col("amount").diff().over("code").cast(pl.UInt64) * 10000,  # change amount value to 0.0001 Yuan
            pl.col("num_trades").diff().over("code").cast(pl.UInt32),
        )
        .rename({"num_trades": "trades_count"})
        .filter(pl.col("volume").is_not_null())
    )

    # time base
    # [09:31, ...11:30]
    # [13:01, ...15:00]
    dft = pl.concat(
        [
            pl.Series([dt.datetime.combine(target_dt, dt.time(9, 26))], dtype=pl.Datetime(time_unit="ms")),
            pl.datetime_range(
                start=dt.datetime.combine(target_dt, dt.time(9, 30)),
                end=dt.datetime.combine(target_dt, dt.time(11, 30)),
                closed="right",
                interval="1m",
                time_unit="ms",
                eager=True,
            ),
            pl.datetime_range(
                start=dt.datetime.combine(target_dt, dt.time(13, 0)),
                end=dt.datetime.combine(target_dt, dt.time(15, 0)),
                closed="right",
                interval="1m",
                time_unit="ms",
                eager=True,
            ),
        ]
    ).to_frame(name="dt")
    # Cartesian product of uniqute code x datetime
    dft_base = df.select(pl.col("code").unique()).join(dft, how="cross")

    # df_bar1m aligned for single date
    df_aligned_bar1m = (
        dft_base.join(df_bar1m, on=["code", "dt"], how="left")
        .with_columns(
            pl.col("volume").fill_null(strategy="zero"),
            pl.col("amount").fill_null(strategy="zero"),
            pl.col("trades_count").fill_null(strategy="zero"),
            pl.col("close").fill_null(strategy="forward").fill_null(strategy="backward").over("code"),
            pl.col("preclose").fill_null(strategy="forward").fill_null(strategy="backward").over("code"),
        )
        .with_columns(
            pl.col("open").fill_null(pl.col("close")).over("code"),
            pl.col("high").fill_null(pl.col("close")).over("code"),
            pl.col("low").fill_null(pl.col("close")).over("code"),
        )
    ).sort(by=["code", "dt"])

    final_dir = f"{out_dir}/{target_dt.year}"  # etf-bar1m/2022
    os.makedirs(final_dir, exist_ok=True)
    out_file = f"{final_dir}/{target_date}.ipc"  # etf-bar1m/2022/20220104.ipc
    df_aligned_bar1m.write_ipc(out_file, compression="zstd")


# bar1m to bar5m, bar10m, bar30m, bar1h, bar2h
def gen_bar(target_date: int, minute_interval: int, in_dir: str, out_dir: str):
    target_dt = dt.date(target_date // 10000, (target_date // 100) % 100, target_date % 100)
    in_file = target_dt.strftime(f"{in_dir}/%Y/%Y%m%d.ipc")  # etf-bar1m/2022/20220104.ipc
    df_aligned_bar1m = pl.read_ipc(in_file, memory_map=False)  # read bar1m, df is sorted by [code, dt]

    df_bar = (
        df_aligned_bar1m.filter(pl.col("dt").dt.time() > dt.time(9, 30))
        .with_columns(
            ((pl.cum_count("dt") - 1).over("code") // minute_interval).alias("count"),
        )
        .group_by(["code", "count"])
        .agg(
            [
                pl.last("dt"),
                pl.last("preclose"),
                pl.first("open"),
                pl.max("high"),
                pl.min("low"),
                pl.last("close"),
                pl.sum("volume"),
                pl.sum("amount"),
                pl.sum("trades_count"),
            ]
        )
        .select(pl.exclude("count"))
    ).sort(by=["code", "dt"])

    final_dir = f"{out_dir}/{target_dt.year}"  # etf-barXm/2022
    os.makedirs(final_dir, exist_ok=True)
    out_file = f"{final_dir}/{target_date}.ipc"  # etf-barXm/2022/20220104.ipc
    df_bar.write_ipc(out_file, compression="zstd")


def do_convert(secu_type: str, minute_interval: int, target_dates: list[int]):
    if minute_interval == 1:
        # bar1m is special alignment
        in_dir = f"transfer/{secu_type}-tick"  # transfer/etf-tick/
        out_dir = f"transfer/{secu_type}-bar1m"  # transfer/etf-bar1m/
        converter = gen_bar1m
    else:
        in_dir = f"transfer/{secu_type}-bar1m"
        out_dir = f"transfer/{secu_type}-bar{minute_interval}m"  # transfer/etf-bar5m/
        converter = gen_bar

    for target_date in target_dates:
        in_file = f"{in_dir}/{target_date // 10000}/{target_date}.ipc"
        if not os.path.exists(in_file):
            print(f"{in_file} not exist")
            continue

        try:
            converter(target_date, minute_interval, in_dir, out_dir)
        except Exception as e:
            print(f"convert to bar{minute_interval}m of {in_file} failed, {e}")
    print(f"{secu_type}:tick:{target_dates[0]}~{target_dates[-1]} combiner done")


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
    do_convert(args.secu_type, args.minute_interval, target_dates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="tick to bar")
    parser.add_argument("-dts", type=int, required=True, dest="dt_start", help="start date, 20070104")
    parser.add_argument("-dte", type=int, required=True, dest="dt_end", help="end date, 20240504")
    parser.add_argument("-mi", type=int, required=True, dest="minute_interval", choices=[1, 5, 15, 30, 60, 120], help="bar interval in minutes: 1, 5, 15, 30, 60, 120")
    parser.add_argument("-st", type=str, dest="secu_type", default="etf", choices=["stock", "etf"], help="security type")

    args = parser.parse_args()
    process(args)

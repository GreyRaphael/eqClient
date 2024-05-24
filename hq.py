import os
import io
import threading
from queue import Queue
import configparser
import logging
import datetime as dt
import argparse
import json
import eqapi
import polars as pl


def get_logger(name: str, level=logging.DEBUG, fmt="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s"):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    file_handler = logging.FileHandler(f"{dt.date.today()}_{name}.log")
    formatter = logging.Formatter(fmt)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger


hq_logger = get_logger("hq")
eq_logger = get_logger("eq", fmt="")


class HistoryApp(eqapi.HqApplication):
    def __init__(self, q: Queue):
        hq_setting = self._read_config("hq.cfg")
        super().__init__([hq_setting, hq_setting])
        self._quotes_q = q

    def _read_config(self, configfile: str) -> eqapi.EqSetting:
        parser = configparser.ConfigParser()
        parser.optionxform = str  # 保持大小写
        parser.read(configfile)
        conf = parser._sections["default"]  # key-value OrderDict

        setting = eqapi.EqSetting()
        setting.ip = conf["ip"]
        setting.port = conf["port"]
        setting.user = conf["user"]
        setting.passwd = conf["passwd"]
        return setting

    def onConnect(self, msg):
        eq_logger.info(msg)

    def onDisconnect(self, msg):
        eq_logger.info(msg)

    def onQuote(self, quotes):
        self._quotes_q.put(quotes)

    def onError(self, msg):
        eq_logger.error(msg)

    def onLog(self, msg):
        eq_logger.debug(msg)


def kl1m_worker(q: Queue, output_dir: str):
    count = 0
    while True:
        quotes = q.get()
        count += 1

        with io.StringIO("\n".join(quotes)) as mem_file:
            df = pl.read_ndjson(
                mem_file,
                schema={
                    "0": pl.Utf8,
                    "3": pl.Int32,
                    "4": pl.Int32,
                    "101": pl.Int32,  # open, int64
                    "102": pl.Int32,  # high, int64
                    "103": pl.Int32,  # low, int64
                    "104": pl.Int32,  # last, int64
                    "112": pl.Int32,  # num_trades, int64
                    "113": pl.Int64,
                    "114": pl.Int64,
                },
            ).rename(
                {
                    "0": "code",
                    "3": "date",
                    "4": "time",
                    "101": "open",
                    "102": "high",
                    "103": "low",
                    "104": "last",
                    "112": "num_trades",
                    "113": "volume",
                    "114": "amount",
                }
            )
            year = df.item(0, "date") // 10000
            os.makedirs(f"{output_dir}/{year}", exist_ok=True)
            df.write_parquet(f"{output_dir}/{year}/{count:08d}.parquet")
        q.task_done()
        hq_logger.debug(f"===>finish {len(quotes)} quotes")


def download_kl1m(line: str, year: int, q: Queue):
    hq_logger.info(f"start {year}")
    hq_app = HistoryApp(q)
    hq_app.start()
    with open(f"calendar/{year}.json") as file:
        date_ints = json.load(file)
    for target_date in date_ints:
        hq_app.get(
            line=line,
            startDate=target_date,
            startTime=92500000,
            endDate=target_date,
            endTime=150000000,
            rate=-1,  # unsorted
        )
    hq_app.wait()
    hq_logger.debug(f"hq_app finish downloading {year}")
    hq_app.stop()
    hq_logger.debug("hq_app disconnect from server.")


def tick_worker(q: Queue, output_dir: str):
    count = 0
    while True:
        quotes = q.get()
        count += 1

        # sort the quotes by length, long -> short
        quotes.sort(key=len, reverse=True)
        with io.StringIO("\n".join(quotes)) as mem_file:
            df = pl.read_ndjson(
                mem_file,
                schema={
                    "0": pl.Utf8,  # code char[16]
                    "3": pl.Int32,  # date int32
                    "4": pl.Int32,  # time int32
                    "100": pl.Int32,  # preclose Int64
                    "101": pl.Int32,  # open Int64
                    # "102": pl.Int32,  # high Int64
                    # "103": pl.Int32,  # low Int64
                    "104": pl.Int32,  # last Int64
                    "108": pl.List(pl.Int32),  # ask_prices Int64[10]
                    "109": pl.List(pl.Int32),  # ask_volumes Int64[10]
                    "110": pl.List(pl.Int32),  # bid_prices Int64[10]
                    "111": pl.List(pl.Int32),  # bid_volumes Int64[10]
                    "112": pl.Int32,  # num_trades Int64
                    "113": pl.Int64,  # volume int64
                    "114": pl.Int64,  # amount Int64
                    "115": pl.Int64,  # total_bid_volume int64
                    "116": pl.Int32,  # bid_avg_price Int64
                    "118": pl.Int64,  # total_ask_volume int64
                    "119": pl.Int32,  # ask_avg_price Int64
                    # "123": pl.Int32,  # high_limit Int64, since 2018
                    # "124": pl.Int32,  # low_limit Int64, since 2018
                },
            ).rename(
                {
                    "0": "code",
                    "3": "date",
                    "4": "time",
                    "100": "preclose",
                    "101": "open",
                    # "102": "high",
                    # "103": "low",
                    "104": "last",
                    "108": "ask_prices",
                    "109": "ask_volumes",
                    "110": "bid_prices",
                    "111": "bid_volumes",
                    "112": "num_trades",
                    "113": "volume",
                    "114": "amount",
                    "115": "total_bid_volume",
                    "116": "bid_avg_price",
                    "118": "total_ask_volume",
                    "119": "ask_avg_price",
                    # "123": "high_limit",
                    # "124": "low_limit",
                }
            )
            year_month = df.item(0, "date") // 100
            os.makedirs(f"{output_dir}/{year_month}", exist_ok=True)
            df.write_parquet(f"{output_dir}/{year_month}/{count:08d}.parquet")
        q.task_done()
        hq_logger.debug(f"===>finish {len(quotes)} quotes")


def get_month_dates(year_month: int) -> list:
    year = year_month // 100
    with open(f"calendar/{year}.json", "r") as file:
        year_dates = json.load(file)
    return [i for i in year_dates if i // 100 == year_month]


def download_tick(line: str, year_month: int, q: Queue):
    hq_logger.info(f"start {year_month}")
    hq_app = HistoryApp(q)
    hq_app.start()

    for target_date in get_month_dates(year_month):
        hq_app.get(
            line=line,
            startDate=target_date,
            startTime=92500000,
            endDate=target_date,
            endTime=150100000,
            rate=-1,  # unsorted
        )
    hq_app.wait()
    hq_logger.debug(f"hq_app finish downloading {year_month}")
    hq_app.stop()
    hq_logger.info("hq_app disconnect from server.")


def run_kl1m_downloader(args):
    if args.etf:
        line = "shkl:kl1m:@510.*|@511.*|@512.*|@513.*|@515.*|@516.*|@517.*|@518.*|@560.*|@561.*|@562.*|@563.*|@588.*+szkl:kl1m:@159.*"
        out_dir = "kl1m/etf"
    else:
        line = "shkl:kl1m:@60.*|@68.*+szkl:kl1m:@00.*|@30.*"
        out_dir = "kl1m/stocks"

    q = Queue(maxsize=64)
    threading.Thread(target=kl1m_worker, args=(q, out_dir), daemon=True).start()
    for year_int in range(args.yr_start, args.yr_end + 1):
        download_kl1m(line, year_int, q)
    q.join()
    hq_logger.info("kl1m_worker finish processing.")


def gen_ym_list(ym_start: int, ym_end: int) -> list:
    year_start = ym_start // 100
    year_end = ym_end // 100
    ym_list = [year * 100 + month for year in range(year_start, year_end + 1) for month in range(1, 13)]
    return [ym for ym in ym_list if ym_start <= ym <= ym_end]


def run_tick_downloader(args):
    if args.etf:
        line = "shl2:tick:@510.*|@511.*|@512.*|@513.*|@515.*|@516.*|@517.*|@518.*|@560.*|@561.*|@562.*|@563.*|@588.*+szl2:tick:@159.*"
        out_dir = "tick/etf"
    else:
        line = "shl2:tick:@60.*|@68.*+szl2:tick:@00.*|@30.*"
        out_dir = "tick/stocks"

    q = Queue(maxsize=32)
    threading.Thread(target=tick_worker, args=(q, out_dir), daemon=True).start()
    for year_month in gen_ym_list(args.ym_start, args.ym_end):
        download_tick(line, year_month, q)
    q.join()
    hq_logger.info("tick_worker finish processing.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="history quotes downloader")
    subparsers = parser.add_subparsers(title="subcommands", required=True)

    kl1m_parser = subparsers.add_parser("kl1m", help="download kl1m")
    kl1m_parser.add_argument("--yr-start", type=int, required=True, help="start year, 2021")
    kl1m_parser.add_argument("--yr-end", type=int, required=True, help="end year, 2023")
    kl1m_parser.add_argument("--etf", action="store_true", help="flag, if set 'etf' else 'stocks'")
    kl1m_parser.set_defaults(func=run_kl1m_downloader)

    tick_parser = subparsers.add_parser("tick", help="download tick")
    tick_parser.add_argument("--ym-start", type=int, required=True, help="start month batch, 202301")
    tick_parser.add_argument("--ym-end", type=int, required=True, help="end month batch, 202412")
    tick_parser.add_argument("--etf", action="store_true", help="flag, if set 'etf' else 'stocks'")
    tick_parser.set_defaults(func=run_tick_downloader)

    args = parser.parse_args()
    args.func(args)

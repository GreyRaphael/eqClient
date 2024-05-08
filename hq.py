import os
import io
import threading
from queue import Queue
import configparser
import logging
import argparse
import json
import eqapi
import polars as pl

logger = logging.getLogger(__name__)
logging.basicConfig(filename="history_quotes.log", format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s", level=logging.DEBUG)


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
        print(msg)

    def onDisconnect(self, msg):
        print(msg)

    def onQuote(self, quotes):
        self._quotes_q.put(quotes)

    def onError(self, msg):
        print(msg)

    def onLog(self, msg):
        print(msg)


def kl1m_worker(q: Queue, target_date: int, output_dir: str):
    year = target_date // 10000
    output_loc = f"{output_dir}/{year}/{target_date}"
    os.makedirs(output_loc, exist_ok=True)
    count = 0
    while True:
        quotes = q.get()
        count += 1

        mem_file = io.StringIO("\n".join(quotes))
        pl.read_ndjson(
            mem_file,
            schema={
                "0": pl.Utf8,
                "3": pl.Int32,
                "4": pl.Int32,
                "101": pl.Int64,
                "102": pl.Int64,
                "103": pl.Int64,
                "104": pl.Int64,
                "112": pl.Int64,
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
        ).write_parquet(f"{output_loc}/{count:08d}.parquet")
        q.task_done()
        logger.debug(f"===>finish {len(quotes)} quotes")


def download_kl1m(line: str, target_date: int, output_dir: str):
    q = Queue(maxsize=1024)
    hq_app = HistoryApp(q)
    hq_app.start()
    threading.Thread(target=kl1m_worker, args=(q, target_date, output_dir), daemon=True).start()

    hq_app.get(
        line=line,
        startDate=target_date,
        startTime=92500000,
        endDate=target_date,
        endTime=150000000,
        rate=-1,  # unsorted
    )
    hq_app.wait()
    q.join()
    logger.info(f"hq_app finish write {target_date}")
    hq_app.stop()
    logger.info("hq_app disconnect from server.")


def tick_worker(q: Queue, target_date: int, output_dir: str):
    os.makedirs(f"{output_dir}/{target_date}", exist_ok=True)
    count = 0
    while True:
        quotes = q.get()
        count += 1

        mem_file = io.StringIO("\n".join(quotes))
        pl.read_ndjson(
            mem_file,
            schema={
                "0": pl.Utf8,  # code char[16]
                "3": pl.Int32,  # date int32
                "4": pl.Int32,  # time int32
                "100": pl.Int64,  # preclose Int64
                "101": pl.Int64,  # open Int64
                # "102": pl.Int64,  # high Int64
                # "103": pl.Int64,  # low Int64
                "104": pl.Int64,  # last Int64
                "108": pl.List(pl.Int64),  # ask_prices Int64[10]
                "109": pl.List(pl.Int64),  # ask_volumes Int64[10]
                "110": pl.List(pl.Int64),  # bid_prices Int64[10]
                "111": pl.List(pl.Int64),  # bid_volumes Int64[10]
                "112": pl.Int64,  # num_trades Int64
                "113": pl.Int64,  # volume int64
                "114": pl.Int64,  # amount Int64
                "115": pl.Int64,  # total_bid_volume int64
                "116": pl.Int64,  # bid_avg_price Int64
                "118": pl.Int64,  # total_ask_volume int64
                "119": pl.Int64,  # ask_avg_price Int64
                # "123": pl.Int64,  # high_limit Int64, since 2018
                # "124": pl.Int64,  # low_limit Int64, since 2018
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
        ).write_parquet(f"{output_dir}/{target_date}/{count:08d}.parquet")
        q.task_done()
        logger.debug(f"===>finish {len(quotes)} quotes")


def download_tick(line: str, target_date: int, output_dir: str):
    q = Queue(maxsize=1024)
    hq_app = HistoryApp(q)
    hq_app.start()
    threading.Thread(target=tick_worker, args=(q, target_date, output_dir), daemon=True).start()

    hq_app.get(
        line=line,
        startDate=target_date,
        startTime=0,
        endDate=target_date,
        endTime=0,
        rate=-1,  # unsorted
    )
    hq_app.wait()
    q.join()
    logger.info(f"hq_app finish write {target_date}")
    hq_app.stop()
    logger.info("hq_app disconnect from server.")


def run_kl1m_downloader(args):
    if args.etf:
        line = "shkl:kl1m:@510.*|@511.*|@512.*|@513.*|@515.*|@516.*|@517.*|@518.*|@560.*|@561.*|@562.*|@563.*|@588.*+szkl:kl1m:@159.*"
        out_dir = "kl1m/etf"
    else:
        line = "shkl:kl1m:@60.*|@68.*+szkl:kl1m:@00.*|@30.*"
        out_dir = "kl1m/stocks"

    for year_int in range(args.yr_start, args.yr_end + 1):
        logger.info(f"start {year_int}")
        with open(f"calendar/{year_int}.json", "r") as file:
            date_ints = json.load(file)
        for target_date in date_ints:
            download_kl1m(line, target_date, output_dir=out_dir)
        logger.info(f"finish {year_int}")


def run_tick_downloader(args):
    if args.etf:
        line = "shkl:kl1m:@510.*|@511.*|@512.*|@513.*|@515.*|@516.*|@517.*|@518.*|@560.*|@561.*|@562.*|@563.*|@588.*+szkl:kl1m:@159.*"
        out_dir = "tick/etf"
    else:
        line = "shkl:kl1m:@60.*|@68.*+szkl:kl1m:@00.*|@30.*"
        out_dir = "tick/stocks"

    for day in range(1, 32):  # to be tested
        target_date = args.ym * 100 + day  # 20240401
        logger.info(f"start {target_date}")
        download_tick(line, target_date, output_dir=out_dir)
        logger.info(f"finish {target_date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="history quotes downloader")
    subparsers = parser.add_subparsers(title="subcommands", required=True)

    kl1m_parser = subparsers.add_parser("kl1m", help="download kl1m")
    kl1m_parser.add_argument("--yr-start", type=int, required=True, help="start year, 2021")
    kl1m_parser.add_argument("--yr-end", type=int, required=True, help="end year, 2023")
    kl1m_parser.add_argument("--etf", action="store_true", help="flag, if set 'etf' else 'stocks'")
    kl1m_parser.set_defaults(func=run_kl1m_downloader)

    tick_parser = subparsers.add_parser("tick", help="download tick")
    tick_parser.add_argument("--ym", type=int, required=True, help="target year month, 202401")
    tick_parser.add_argument("--etf", action="store_true", help="flag, if set 'etf' else 'stocks'")
    tick_parser.set_defaults(func=run_tick_downloader)

    args = parser.parse_args()
    args.func(args)

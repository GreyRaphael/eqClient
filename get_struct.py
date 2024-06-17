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
from utils import chatbot


def get_logger(name: str, level=logging.DEBUG, fmt="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s"):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    os.makedirs("log", exist_ok=True)
    file_handler = logging.FileHandler(f'log/{dt.datetime.now().strftime("%Y%m%d-%H%M")}_{name}.log')
    formatter = logging.Formatter(fmt)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger


class HistoryApp(eqapi.HqApplication):
    def __init__(self, q: Queue):
        hq_setting = self._read_config("hq.cfg")
        super().__init__([hq_setting, hq_setting])
        self._quotes_q = q
        self.eq_logger = get_logger("eq", fmt="%(asctime)s - %(message)s")
        self.hq_logger = logging.getLogger("hq")

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
        self.eq_logger.info(msg)

    def onDisconnect(self, msg):
        self.eq_logger.info(msg)

    def onQuote(self, quotes):
        self._quotes_q.put(quotes)
        head_j = json.loads(quotes[0])
        self.hq_logger.debug(f"receive {len(quotes)} quotes from server at {head_j['3']}")

    def onError(self, msg):
        self.eq_logger.error(msg)

    def onLog(self, msg):
        self.eq_logger.debug(msg)
        if "Server Log: data complete, total" in msg:
            idx = msg.find("Server Log")
            self.hq_logger.info(msg[idx:])


def worker(q: Queue, schema_mapping: dict, name_mapping: dict, output_dir: str):
    count = 0
    hq_logger = logging.getLogger("hq")
    while True:
        quotes = q.get()
        count += 1

        # sort the quotes by length, long -> short
        quotes.sort(key=len, reverse=True)

        with io.BytesIO() as mem_file:
            for quote in quotes:
                mem_file.write(quote.encode())
                mem_file.write(b"\n")

            df = pl.read_ndjson(mem_file, schema_overrides=schema_mapping).rename(name_mapping)

        current_date = df.item(0, "date")
        os.makedirs(f"{output_dir}/{current_date}", exist_ok=True)
        df.write_parquet(f"{output_dir}/{current_date}/{count:08d}.parquet")
        q.task_done()
        hq_logger.debug(f"===>finish {len(quotes)} quotes of {current_date}")


def download(line: str, target_dates: list[int]):
    chatbot.send_msg(f"begin {line} from {target_dates[0]} to {target_dates[-1]}")
    hq_logger = get_logger("hq")

    out_dir = line.replace(":", "_")  # shl2_tick_510050
    hq_logger.debug(f"output dir: {out_dir}, quote line: {line}")

    schema = {
        "0": pl.Utf8,  # code char[16]
        "3": pl.Int32,  # date int32
        "4": pl.Int32,  # time int32
        "100": pl.Int32,  # preclose Int64
        "101": pl.Int32,  # open Int64
        "102": pl.Int32,  # high Int64
        "103": pl.Int32,  # low Int64
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
    }
    name_mapping = {
        "0": "code",
        "3": "date",
        "4": "time",
        "100": "preclose",
        "101": "open",
        "102": "high",
        "103": "low",
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
    }

    q = Queue(maxsize=64)
    hq_app = HistoryApp(q)
    threading.Thread(target=worker, args=(q, schema, name_mapping, out_dir), daemon=True).start()
    hq_app.start()

    for target_date in target_dates:
        hq_logger.debug(f"hq_app begin {target_date}")
        hq_app.get(
            line=line,
            startDate=target_date,
            startTime=93000000,
            endDate=target_date,
            endTime=93015000,
            rate=-1,  # unsorted
        )
        hq_app.wait()
        hq_logger.info(f"hq_app downloaded {target_date}")
    q.join()
    hq_logger.debug(f"worker finish processing {target_dates[0]}~{target_dates[-1]}")
    hq_app.stop()
    hq_logger.info("hq_app disconnect from server.")
    chatbot.send_msg(f"finish {line} from {target_dates[0]} to {target_dates[-1]}")


def get_target_dates(ym_start: int, ym_end: int) -> list[int]:
    year_start = ym_start // 100
    year_end = ym_end // 100
    target_dates = []
    for year in range(year_start, year_end + 1):
        with open(f"calendar/{year}.json", "r") as file:
            target_dates += json.load(file)
    return [i for i in target_dates if ym_start * 100 < i <= ym_end * 100 + 31]


def process(args):
    target_dates = get_target_dates(args.ym_start, args.ym_end)
    download(args.line, target_dates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="history quotes downloader")
    parser.add_argument("-yms", type=int, required=True, dest="ym_start", help="start year-month, 200701")
    parser.add_argument("-yme", type=int, required=True, dest="ym_end", help="end year-month, 202412")
    parser.add_argument("-line", type=str, required=True, dest="line", choices=["shl2:tick:510050", "shl2:tick:600651", "szl2:tick:159902", "szl2:tick:000001"], help="quote line")

    args = parser.parse_args()
    process(args)

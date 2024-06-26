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
        self.eq_logger = get_logger("eqlimit", fmt="%(asctime)s - %(message)s")
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

            df = pl.read_ndjson(mem_file, schema=schema_mapping).rename(name_mapping)

        current_date = df.item(0, "date")
        os.makedirs(f"{output_dir}/{current_date}", exist_ok=True)
        df.write_parquet(f"{output_dir}/{current_date}/{count:08d}.parquet")
        q.task_done()
        hq_logger.debug(f"===>finish {len(quotes)} quotes of {current_date}")


def get_codes(secu_type: str) -> tuple:
    """get code line from json file"""
    if secu_type == "etf":
        with open("codes/etf.json") as file:
            j_codes = json.load(file)

        # sh_line = "@510.*|@511.*|@512.*|@513.*|@515.*|@516.*|@517.*|@518.*|@560.*|@561.*|@562.*|@563.*|@588.*" # bad
        sh_line = "|".join(j_codes["sh"])
        # sz_line = "@159.*" # bad
        sz_line = "|".join(j_codes["sz"])
    else:
        sh_line = "@60.*|@68.*"
        sz_line = "@00.*|@30.*"

    return sh_line, sz_line


def download(secu_type: str, quote_type: str, target_dates: list[int]):
    """
    Download the quotes in the target_dates list, where the quotes meet the secu_type and quote_type.\n\n
    Args:
        secu_type: str, example: etf, stock
        quote_type: str, example: kl1m, tick
        target_dates: list[int], example: [20220101, 20220102, ...]
    """
    chatbot.send_msg(f"begin {secu_type}:{quote_type} from {target_dates[0]} to {target_dates[-1]}")
    hq_logger = get_logger("hqlimit")

    out_dir = f"{secu_type}-limit/{quote_type}"  # etf/tick/
    hq_logger.debug(f"output dir: {out_dir}")

    if quote_type == "kl1m":
        eq_line = "kl:kl1m"
        qsize = 128
        schema = {
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
        }
        name_mapping = {
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
    else:
        eq_line = "l2:tick"
        qsize = 64
        schema = {
            "0": pl.Utf8,  # code char[16]
            "3": pl.Int32,  # date int32
            "4": pl.Int32,  # time int32
            "100": pl.Int32,  # preclose Int64
            "101": pl.Int32,  # open Int64
            "123": pl.Int32,  # high_limit Int64, since 2018
            "124": pl.Int32,  # low_limit Int64, since 2018
        }
        name_mapping = {
            "0": "code",
            "3": "date",
            "4": "time",
            "100": "preclose",
            "101": "open",
            "123": "high_limit",
            "124": "low_limit",
        }

    sh_line, sz_line = get_codes(secu_type)
    line = f"sh{eq_line}:{sh_line}+sz{eq_line}:{sz_line}"
    hq_logger.debug(f"quote line: {line}")

    q = Queue(maxsize=qsize)
    hq_app = HistoryApp(q)
    threading.Thread(target=worker, args=(q, schema, name_mapping, out_dir), daemon=True).start()
    hq_app.start()

    for target_date in target_dates:
        hq_logger.debug(f"hq_app begin {target_date}")
        hq_app.get(
            line=line,
            startDate=target_date,
            startTime=92500000,
            endDate=target_date,
            endTime=150100000,
            rate=-1,  # unsorted
        )
        hq_app.wait()
        hq_logger.info(f"hq_app downloaded {target_date}")
    q.join()
    hq_logger.debug(f"worker finish processing {target_dates[0]}~{target_dates[-1]}")
    hq_app.stop()
    hq_logger.info("hq_app disconnect from server.")
    chatbot.send_msg(f"finish {secu_type}:{quote_type} from {target_dates[0]} to {target_dates[-1]}")


def process(args):
    target_dates = [args.target_dt]
    download(args.secu_type, args.quote_type, target_dates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="history quotes downloader")
    parser.add_argument("-dt", type=int, required=True, dest="target_dt", help="target date 20240614")
    parser.add_argument("-st", type=str, required=True, dest="secu_type", choices=["stock", "etf"], help="security type")
    parser.add_argument("-qt", type=str, required=True, dest="quote_type", choices=["tick", "kl1m"], help="quote type")

    args = parser.parse_args()
    process(args)

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
        # quotes.sort(key=len, reverse=True)
        # quotes.sort(key=lambda x: (x.find('],"153":['), x.count('":')), reverse=True)
        quotes.sort(key=lambda x: x.find('],"153":['), reverse=True)

        with io.BytesIO() as mem_file:
            for quote in quotes:
                mem_file.write(quote.encode())
                mem_file.write(b"\n")

            try:
                df = pl.read_ndjson(mem_file, schema=schema_mapping).rename(name_mapping)
            except Exception as e:
                chatbot.send_msg(f"read_ndjson parse error, {e}, exiting.")
                with open("mem_file.json", "wb") as file:
                    file.write(mem_file.getbuffer())
                os._exit(0)

        current_date = df.item(0, "date")
        os.makedirs(f"{output_dir}/{current_date}", exist_ok=True)
        df.write_parquet(f"{output_dir}/{current_date}/{count:08d}.parquet")
        q.task_done()
        hq_logger.debug(f"===>finish {len(quotes)} quotes of {current_date}")


def get_codes(secu_type: str) -> tuple:
    """get code line from json file"""
    if secu_type == "etf":
        with open("etf_list.json") as file:
            j_codes = json.load(file)

        # sh_codes = "@510.*|@511.*|@512.*|@513.*|@515.*|@516.*|@517.*|@518.*|@520.*|@560.*|@561.*|@562.*|@563.*|@588.*"  # bad
        # sz_codes = "@159.*"  # bad
        sh_codes = "|".join(j_codes["sh"])
        sz_codes = "|".join(j_codes["sz"])
    else:
        sh_codes = "@60.*|@68.*"
        sz_codes = "@00.*|@30.*"

    return sh_codes, sz_codes


def download(secu_type: str, quote_type: str, target_dates: list[int]):
    """
    Download the quotes in the target_dates list, where the quotes meet the secu_type and quote_type.\n\n
    Args:
        secu_type: str, example: etf, stock
        quote_type: str, example: kl1m, tick
        target_dates: list[int], example: [20220101, 20220102, ...]
    """
    chatbot.send_msg(f"begin {secu_type}:{quote_type} from {target_dates[0]} to {target_dates[-1]}")
    hq_logger = get_logger("hq")

    out_dir = f"{secu_type}/{quote_type}"  # etf/tick/
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
    elif quote_type == "tick":
        eq_line = "l2:tick"
        qsize = 64
        schema = {
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
            "123": pl.Int32,  # high_limit Int64, since 2018
            "124": pl.Int32,  # low_limit Int64, since 2018
            "151": pl.List(pl.Int32),  # bid_nums
            "153": pl.List(pl.Int32),  # ask_nums
            "201": pl.Int32,  # iopv (only for etf, stock is null)
        }
        name_mapping = {
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
            "123": "high_limit",
            "124": "low_limit",
            "151": "bid_nums",
            "153": "ask_nums",
            "201": "iopv",
        }
    elif quote_type == "trade":
        eq_line = "l2:trade"
        qsize = 64
        schema = {
            "0": pl.Utf8,
            "3": pl.Int32,
            "4": pl.Int32,
            "600": pl.Int64,
            "601": pl.Int64,
            "602": pl.Int64,
            "604": pl.Utf8,
            "605": pl.Int64,
            "606": pl.Int64,
        }
        name_mapping = {
            "0": "code",
            "3": "date",
            "4": "time",
            "600": "seq_no",
            "601": "price",
            "602": "volume",
            "604": "bs_flag",
            "605": "ask_seq_no",
            "606": "bid_seq_no",
        }
    elif quote_type == "order":
        eq_line = "l2:order"
        qsize = 64
        schema = {
            "0": pl.Utf8,
            "3": pl.Int32,
            "4": pl.Int32,
            "700": pl.Int64,
            "701": pl.Int64,
            "702": pl.Int64,
            "703": pl.Utf8,
            "704": pl.Utf8,
            "710": pl.Int32,
        }
        name_mapping = {
            "0": "code",
            "3": "date",
            "4": "time",
            "700": "seq_no",
            "701": "price",
            "702": "volume",
            "703": "bs_flag",
            "704": "order_type",
            "710": "orgin_seq_no",
        }
    else:
        raise ValueError(f"unknown quote_type: {quote_type}")

    sh_codes, sz_codes = get_codes(secu_type)
    sh_line = f"sh{eq_line}:{sh_codes}"
    hq_logger.debug(f"quote line: {sh_line}")
    sz_line = f"sz{eq_line}:{sz_codes}"
    hq_logger.debug(f"quote line: {sz_line}")

    q = Queue(maxsize=qsize)
    hq_app = HistoryApp(q)
    threading.Thread(target=worker, args=(q, schema, name_mapping, out_dir), daemon=True).start()
    hq_app.start()

    for target_date in target_dates:
        hq_logger.debug(f"hq_app begin {target_date}")
        # sh
        hq_app.get(
            line=sh_line,
            startDate=target_date,
            startTime=92500000,
            endDate=target_date,
            endTime=150100000,
            rate=-1,  # unsorted
        )
        hq_app.wait()
        # sz
        hq_app.get(
            line=sz_line,
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


def get_target_dates(date_start: int, date_end: int) -> list[int]:
    year_start = date_start // 10000
    year_end = date_end // 10000
    target_dates = []
    for year in range(year_start, year_end + 1):
        with open(f"calendar/{year}.json", "r") as file:
            target_dates += json.load(file)
    return [i for i in target_dates if date_start <= i <= date_end]


def process(args):
    target_dates = get_target_dates(args.date_start, args.date_end)
    download(args.secu_type, args.quote_type, target_dates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="history quotes downloader")
    parser.add_argument("-dts", type=int, required=True, dest="date_start", help="start date, 20070101")
    parser.add_argument("-dte", type=int, required=True, dest="date_end", help="end date, 20241231")
    parser.add_argument("-st", type=str, required=True, dest="secu_type", choices=["stock", "etf"], help="security type")
    parser.add_argument("-qt", type=str, required=True, dest="quote_type", choices=["tick", "kl1m", "trade", "order"], help="quote type")

    args = parser.parse_args()
    process(args)

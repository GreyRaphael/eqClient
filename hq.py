import os
import io
import threading
from queue import Queue
import configparser
import logging
import argparse
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
        conf = parser._sections["DEFAULT"]  # key-value OrderDict

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


def kl1m_worker(q: Queue, year: int, output_dir: str):
    os.makedirs(f"{output_dir}/{year}", exist_ok=True)
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
                "101": pl.Int32,
                "102": pl.Int32,
                "103": pl.Int32,
                "104": pl.Int32,
                "112": pl.Int32,
                "113": pl.Int64,
                "114": pl.Float64,
            },
        ).rename(
            {
                "0": "secucode",
                "3": "date",
                "4": "time",
                "101": "open",
                "102": "high",
                "103": "low",
                "104": "close",
                "112": "num_trades",
                "113": "volume",
                "114": "amount",
            }
        ).write_parquet(f"{output_dir}/{year}/{count:08d}.parquet")
        q.task_done()
        logger.debug(f"===>finish {len(quotes)} quotes")


def download_kl1m(line: str, year: int, output_dir: str):
    q = Queue(maxsize=1024)
    hq_app = HistoryApp(q)
    hq_app.start()
    threading.Thread(target=kl1m_worker, args=(q, year, output_dir), daemon=True).start()

    hq_app.get(
        line=line,
        startDate=year * 10000 + 101,
        startTime=0,
        endDate=year * 10000 + 1231,
        endTime=0,
        rate=-1,  # unsorted
    )
    hq_app.wait()
    q.join()
    logger.info(f"hq_app finish write {year}")
    hq_app.stop()
    logger.info("hq_app disconnect from server.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="kl1min downloader")
    parser.add_argument("--yr-start", type=int, required=True, help="start year, 2021")
    parser.add_argument("--yr-end", type=int, required=True, help="end year, 2023")
    parser.add_argument("--etf", action="store_true", help="flag, if set 'etf' else 'stocks'")
    args = parser.parse_args()

    if args.etf:
        line = "shkl:kl1m:@510.*|@511.*|@512.*|@513.*|@515.*|@516.*|@517.*|@518.*|@560.*|@561.*|@562.*|@563.*|@588.*+szkl:kl1m:@159.*"
        out_dir = "etf"
    else:
        line = "shkl:kl1m:@60.*|@68.*+szkl:kl1m:@00.*|@30.*"
        out_dir = "stocks"

    for year_int in range(args.yr_start, args.yr_end + 1):
        logger.info(f"start {year_int}")
        download_kl1m(line, year_int, output_dir=out_dir)
        logger.info(f"finish {year_int}")

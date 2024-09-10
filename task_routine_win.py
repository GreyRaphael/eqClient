import datetime as dt
import argparse
import hq
import dt_combiner
from WindPy import w
from utils import chatbot
import json


def wind_ready() -> bool:
    # check wind connection 10 times
    for _ in range(10):
        w.start()
        if w.isconnected():
            return True
    return False


def get_etf_list(date_str: str) -> tuple:
    # get all etf list(包含未上市和退市的)
    sh_codes, sz_codes = [], []
    response = w.wset("sectorconstituent", f"date={date_str};sectorid=1000051239000000;field=wind_code")
    if response.ErrorCode == 0:
        # response.Data likst [['159001.OF', '159003.OF', '561200.OF',]]
        for raw_code in response.Data[0]:
            if raw_code.startswith("1"):
                # sz starts with 159
                sz_codes.append(raw_code[:6])
            elif raw_code.startswith("5"):
                # sh starts with 5
                sh_codes.append(raw_code[:6])
        return sh_codes, sz_codes

    else:
        chatbot.send_msg(f"WindPy Errorcode: {response.ErrorCode} at {date_str}")
        return [], []


def write_etf_list(current_dt: dt.date | dt.datetime):
    if wind_ready():
        sh_codes, sz_codes = get_etf_list(current_dt.strftime("%Y-%m-%d"))
        with open("etf_list.json", "w") as f:
            json.dump({"sh": sh_codes, "sz": sz_codes}, f)
    else:
        chatbot.send_msg("WindPy not ready")


def collect_trade_days(current_dt: dt.date | dt.datetime) -> list[int]:
    """collect the trading days in the week of current date or datetime"""
    monday = current_dt - dt.timedelta(days=current_dt.weekday())
    this_workdays = [monday + dt.timedelta(days=i) for i in range(5)]
    return [i.year * 10000 + i.month * 100 + i.day for i in this_workdays]


def job_worker(secu_type: str, quote_type: str):
    today = dt.date.today()
    write_etf_list(today)
    target_dates = collect_trade_days(today)
    # download quotes
    hq.download(secu_type, quote_type, target_dates)
    # proces data
    dt_combiner.do_comine(secu_type, quote_type, target_dates)


# run at 16:10 at every Sat etf, tick
# run at 15:10 at every Sat etf, kl1m
# run at 16:45 at every Sun stock, tick, later
# run at 15:10 at every Sun stock, kl1m, later


def process(args):
    job_worker(args.secu_type, args.quote_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="history quotes downloader")
    parser.add_argument("-st", type=str, required=True, dest="secu_type", choices=["stock", "etf"], help="security type")
    parser.add_argument("-qt", type=str, required=True, dest="quote_type", choices=["tick", "kl1m"], help="quote type")

    args = parser.parse_args()
    process(args)

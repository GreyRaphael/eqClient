import datetime as dt
import argparse
from utils import chatbot
import hq


def collect_trade_days(current_dt: dt.date | dt.datetime) -> list[int]:
    """collect the trading days in the week of current date or datetime"""
    monday = current_dt - dt.timedelta(days=current_dt.weekday())
    this_workdays = [monday + dt.timedelta(days=i) for i in range(5)]
    return [i.year * 10000 + i.month * 100 + i.day for i in this_workdays]


def job_worker(secu_type: str, quote_type: str):
    today = dt.date.today()
    target_dates = collect_trade_days(today)
    hq.download(secu_type, quote_type, target_dates)
    chatbot.send_msg(f"finish task of {secu_type} {quote_type} at {target_dates}")


# run at 9:45 at every Sat etf, tick
# run at 18:45 at every Sat etf, kl1m
# run at 9:45 at every Sun stock, tick, later
# run at 18:45 at every Sun stock, kl1m, later


def process(args):
    job_worker(args.secu_type, args.quote_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="history quotes downloader")
    parser.add_argument("-st", type=str, required=True, dest="secu_type", choices=["stock", "etf"], help="security type")
    parser.add_argument("-qt", type=str, required=True, dest="quote_type", choices=["tick", "kl1m"], help="quote type")

    args = parser.parse_args()
    process(args)
import os
import datetime as dt
import hq
import dt_combiner
import genbar
import task_update_etf_list
from utils import chatbot


def is_modified_today(file_path):
    modification_time = os.path.getmtime(file_path)
    modification_date = dt.datetime.fromtimestamp(modification_time)
    return modification_date.date() == dt.date.today()


def job_worker(secu_type: str, quote_type: str, date_int: int):
    # update etf list
    if is_modified_today("etf_list.json"):
        print("using cached etf_list.json")
    else:
        print("updating etf_list.json")
        task_update_etf_list.write_etf_list()

    target_dates = [date_int]
    # download quotes
    hq.download(secu_type, quote_type, target_dates)
    # proces data
    dt_combiner.do_comine(secu_type, quote_type, target_dates)
    # dump bar1m
    genbar.gen_bar1m(target_dates[0], 1, f"transfer/{secu_type}-{quote_type}", f"transfer/{secu_type}-bar1m")
    # dump bar15m
    genbar.gen_bar(target_dates[0], 15, f"transfer/{secu_type}-bar1m", f"transfer/{secu_type}-bar15m")
    chatbot.send_msg(f"finish etf tick to bar1m & bar15m at {date_int}")


# run etf tick at 23:00 at Mon-Fri
if __name__ == "__main__":
    today = dt.date.today()
    date_int = today.year * 10000 + today.month * 100 + today.day
    job_worker("etf", "tick", date_int)

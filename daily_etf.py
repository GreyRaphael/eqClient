import datetime as dt
import hq
import dt_combiner
import genbar


def job_worker():
    today = dt.date.today()
    # check if today 20:00 can donwload today history?
    target_dates = [today - dt.timedelta(days=1)]
    # download quotes
    hq.download("etf", "tick", target_dates)
    # proces data
    dt_combiner.do_comine("etf", "tick", target_dates)
    # gen bar
    target_dateint = today.year * 10000 + today.month * 100 + today.day
    genbar.gen_bar1m(target_dateint, 1, "transfer/etf-tick", "transfer/etf-bar1m")
    genbar.gen_bar(target_dateint, 15, "transfer/etf-bar1m", "transfer/etf-bar15m")


if __name__ == "__main__":
    job_worker()

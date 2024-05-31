import datetime as dt
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler import events
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
    print(f"finish task of {secu_type} {quote_type} at {target_dates}")


def job_listener(event):
    if event.exception:
        print("The job crashed :(")
    else:
        job = scheduler.get_job(event.job_id)
        print(f"Job {event.job_id} completed. Next run time: {job.next_run_time}")


scheduler = BlockingScheduler()
# run at 9:45 at every Sat etf, tick
scheduler.add_job(job_worker, "cron", day_of_week="sat", hour=9, minute=45, args=("etf", "tick"))
# run at 18:45 at every Sat etf, kl1m
scheduler.add_job(job_worker, "cron", day_of_week="sat", hour=18, minute=37, args=("etf", "kl1m"))
# run at 9:45 at every Sun stock, tick
scheduler.add_job(job_worker, "cron", day_of_week="sun", hour=9, minute=45, args=("stock", "tick"))
# run at 18:45 at every Sun stock, kl1m
scheduler.add_job(job_worker, "cron", day_of_week="sun", hour=18, minute=37, args=("stock", "kl1m"))

for job in scheduler.get_jobs():
    print(job)

scheduler.add_listener(job_listener, events.EVENT_JOB_EXECUTED)
scheduler.start()

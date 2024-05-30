from collections import namedtuple
from apscheduler.schedulers.blocking import BlockingScheduler
import hq

Args = namedtuple("Args", ["ym_start", "ym_end", "secu_type", "quote_type"])


def job_worker(args):
    hq.process(args)
    print("finish", args)


task_dict = {
    "2024-05-31 15:07:00": Args(ym_start=201901, ym_end=202012, secu_type="etf", quote_type="tick"),
    "2024-06-01 09:40:00": Args(ym_start=202101, ym_end=202212, secu_type="etf", quote_type="tick"),
    "2024-06-02 09:45:00": Args(ym_start=202301, ym_end=202405, secu_type="etf", quote_type="tick"),
}


scheduler = BlockingScheduler()
for task_date in task_dict:
    arg_np = task_dict[task_date]
    scheduler.add_job(job_worker, "date", run_date=task_date, args=(arg_np,))

for job in scheduler.get_jobs():
    print(job)

scheduler.start()

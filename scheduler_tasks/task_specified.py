from collections import namedtuple
from apscheduler.schedulers.blocking import BlockingScheduler
import hq
from utils import chatbot

Args = namedtuple("Args", ["ym_start", "ym_end", "secu_type", "quote_type"])


def job_worker(args):
    hq.process(args)
    chatbot.send_msg(f"finish {args}")


task_dict = {
    "2024-05-31 15:07:00": Args(ym_start=202101, ym_end=202206, secu_type="etf", quote_type="tick"),
    "2024-06-01 09:51:00": Args(ym_start=202207, ym_end=202312, secu_type="etf", quote_type="tick"),
    "2024-06-02 09:47:00": Args(ym_start=202401, ym_end=202404, secu_type="etf", quote_type="tick"),
}


scheduler = BlockingScheduler()
for task_date in task_dict:
    arg_namesp = task_dict[task_date]
    scheduler.add_job(job_worker, "date", run_date=task_date, args=(arg_namesp,))

for job in scheduler.get_jobs():
    print(job)

scheduler.start()

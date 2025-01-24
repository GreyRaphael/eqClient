import datetime as dt
from cscmail import CscMailUtil


def job_worker(target_dt: dt.date):
    dt_str1 = target_dt.strftime("%Y%m%d")
    dt_str2 = target_dt.strftime("%Y-%m-%d")
    uploader = CscMailUtil("CSCXXXXX", "XXXXX")
    uploader.upload(
        files=[
            f"D:/eqClient/transfer/etf-bar1m/{target_dt.year}/{dt_str1}.ipc",
            f"D:/eqClient/transfer/etf-bar15m/{target_dt.year}/bar15m-{dt_str1}.ipc",
            f"D:/windClient/wind-etf-bar1d/{dt_str2}.ipc",
            f"D:/windClient/wind-lof-bar1d/lof{dt_str2}.ipc",
        ]
    )


if __name__ == "__main__":
    today = dt.date.today() - dt.timedelta(days=1)
    job_worker(today)

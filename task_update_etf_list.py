from WindPy import w
from utils import chatbot
import json
import datetime as dt


def wind_ready() -> bool:
    # check wind connection 30 times
    for _ in range(30):
        w.start()
        if w.isconnected():
            return True
    return False


def get_etf_list(date_str: str) -> list[str]:
    # get all etf list(包含未上市和退市的)
    response = w.wset("sectorconstituent", f"date={date_str};sectorid=1000051239000000;field=wind_code")
    if response.ErrorCode == 0:
        # response.Data likst [['159001.OF', '159003.OF', '561200.OF',]]
        return response.Data[0]
    else:
        chatbot.send_msg(f"WindPy Errorcode: {response.ErrorCode} at {date_str}")
        return []


def write_etf_list():
    today = dt.date.today()
    if wind_ready():
        etf_list = get_etf_list(today.strftime("%Y-%m-%d"))
        with open("D:/etf_list.json", "w") as f:
            json.dump(etf_list, f)
    else:
        chatbot.send_msg("WindPy not ready")


if __name__ == "__main__":
    write_etf_list()

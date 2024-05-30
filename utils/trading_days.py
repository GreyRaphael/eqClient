import argparse
import json
import asyncio
import httpx

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Referer": "http://www.szse.cn/aboutus/calendar/index.html",
}


async def fetch_url(client, url):
    response = await client.get(url, headers=HEADERS, timeout=None)
    print("↓", url)
    return response.json().get("data")


async def fetch_urls(urls: list):
    client = httpx.AsyncClient()
    tasks = [fetch_url(client, url) for url in urls]
    return await asyncio.gather(*tasks)


def download_calendar(year: str, out_int: bool, output_dir: str = "."):
    print("↓ trading days of", year)
    urls = [f"http://www.szse.cn/api/report/exchange/onepersistenthour/monthList?month={year}-{i:02d}" for i in range(1, 13)]
    j_data_list = asyncio.run(fetch_urls(urls))
    print("finish ↓ trading days of", year)

    # write to json
    if out_int:
        trading_days = [int("".join(record["jyrq"].split("-"))) for record_list in j_data_list for record in record_list if record["jybz"] == "1"]
    else:
        trading_days = [record["jyrq"] for record_list in j_data_list for record in record_list if record["jybz"] == "1"]

    with open(f"{output_dir}/{year}.json", "w") as file:
        json.dump(trading_days, file, indent=2)
    print("finish writing trading days of", year)


def download_calendars(args):
    for year in range(args.yr_start, args.yr_end + 1):
        download_calendar(year, args.out_int, output_dir="calendar")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="download trading days")
    parser.add_argument("-yrs", type=int, required=True, dest="yr_start", help="start year, 2021")
    parser.add_argument("-yre", type=int, required=True, dest="yr_end", help="end year, 2023")
    parser.add_argument("-oi", dest="out_int", action="store_true", help="flag, if set 'integer' else 'string'")
    parser.set_defaults(func=download_calendars)

    args = parser.parse_args()
    args.func(args)

import os
import json
import requests


def read_config(filename: str) -> dict:
    """read json file to dictionary"""
    with open(filename, "r", encoding="utf8") as file:
        return json.load(file)


def send_msg(msg: str):
    abs_cfg_file = os.path.join(os.path.dirname(__file__), "chatbot.json")
    CHATBOT_CONFIG = read_config(abs_cfg_file)
    if CHATBOT_CONFIG["isProduct"] is True:
        post_data = {
            "ESBREQ": {
                "HEADER": {
                    "SERVICE_CODE": "S-001029",
                    "VERSION": "1",
                    "CONSUMER_CODE": "CS-SPADE-001",
                    "PASSWORD": "10f33bbdee9249d8ba1f951e80f2924a",
                    "MESSAGE_ID": "SPADE-EMSB-045-webhook_send",
                    "DTSEND": "20150108011000000",
                    "EXT1": f"1111111111{CHATBOT_CONFIG['product']['KEY']}1111111111",
                    "EXT2": "1113111211146097ab-4d24-4533-a289-fb848ea78cca1111111111",
                    "EXT3": "",
                },
                "DATA": {
                    "msgtype": "text",
                    "text": {
                        "content": msg,
                        "mentioned_mobile_list": CHATBOT_CONFIG["product"]["PHONE_LIST"],
                    },
                },
            }
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        with requests.post(CHATBOT_CONFIG["product"]["CHATBOT_URL"], json=post_data, headers=headers) as response:
            jData = response.json()
    else:
        post_data = {
            "msgtype": "text",
            "text": {
                "content": msg,
                "mentioned_mobile_list": CHATBOT_CONFIG["test"]["PHONE_LIST"],
            },
        }
        with requests.post(CHATBOT_CONFIG["test"]["CHATBOT_URL"], json=post_data) as response:
            jData = response.json()
    print("企业微信Response:", jData)


if __name__ == "__main__":
    send_msg("hello")

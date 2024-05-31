import json
import requests


def read_config(filename: str) -> dict:
    """read json file to dictionary"""
    with open(filename, "r", encoding="utf8") as file:
        return json.load(file)


def send_msg(msg: str):
    conf = read_config("chatbot.json")
    print("sending", msg)

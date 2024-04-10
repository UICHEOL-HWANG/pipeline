import requests
import json


def get_data():
    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    result = response['results'][0]
    return result
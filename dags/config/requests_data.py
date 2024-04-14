import requests
import json


# on-premise용
# with open('/Users/uicheol_hwang/Data_Engineering/dags/config/cookie_config.json','r') as file:
#     config_cookie = json.load(file)
#
# with open('/Users/uicheol_hwang/Data_Engineering/dags/config/header_config.json','r') as header_file:
#     config_header = json.load(header_file)

# Docker Stream용
with open('/opt/airflow/dags/config/cookie_config.json','r') as file :
    config_cookie = json.load(file)

with open('/opt/airflow/dags/config/header_config.json','r') as header_file :
    config_header = json.load(header_file)

class GetBookItems:
    def __init__(self):
        self.cookie = config_cookie
        self.header = config_header
        self.url = 'https://product.kyobobook.co.kr/api/gw/pub/pdt/best-seller/total'
        self.imageUrl = 'https://product.kyobobook.co.kr/bestseller/total'

    def get_data(self, page, per, period, ymw, bsslBksClstCode):
        url = self.url
        params = dict(page=page, per=per, period=period, ymw=ymw, bsslBksClstCode=bsslBksClstCode)
        response = requests.get(url, params=params, headers=self.header, cookies=self.cookie)
        semi_struct = json.loads(response.text)

        return semi_struct

    def catch_images(self, page, per, period, ymw, bsslBksClstCode):
        url = self.imageUrl
        params = dict(page=page, per=per, period=period, ymw=ymw, bsslBksClstCode=bsslBksClstCode)
        response = requests.get(url, params=params, headers=self.header, cookies=self.cookie)

        return response.text
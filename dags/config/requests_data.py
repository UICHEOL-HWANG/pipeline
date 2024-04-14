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
    """
    cookie : 쿠키 값을 opt/airflow/dags dir을 통해 저장하였으나 자세한 내용은 json key value를 통해 확인
    header : cookie와 동일
    url,imageUrl : json, image와 별개로 나누어서, 추출하기 위해 사용

    defines
    - get_data : url, params, header, cookie,를 기본 클래스 내로 끌고온 후 각 params config 값을 파라미터로 조정
    - catch_image : get_data 내에서 얻은 링크 값으로 이미지 이름을 통해 jpg 값으로 재 추출
    """
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
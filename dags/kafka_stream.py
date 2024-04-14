from datetime import datetime
from kafka import KafkaProducer
import time
import logging
import json

from config.requests_data import *


# Module concats
links = GetBookItems()

def stream_data_json():
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000,
        value_serializer= lambda x: json.dumps(x).encode('utf-8')
    )
    curr_time = time.time()

    for years in range(2014,2025): # 년도
        for pages in range(1,5): # 50개씩 보기는 4페이지까지만 제공
            if time.time() > curr_time + 60:
                break
            try:
                data = links.get_data(pages, 50, '004', years, 'A')['data']['bestSeller']  # 데이터 가져오기, 이 부분은 데이터 구조에 맞게 수정 필요
                for item in data:
                    if 'productInfo' in item:

                        producer.send('book_product_info',item['productInfo'])

                        tmp_dict = {key: value for key, value in item.items() if key != 'productInfo'} # json parse 2중 작업으로 인해 분리
                        producer.send('book_other_data',tmp_dict)

                        time.sleep(3) # 추출 간격 조정
            except Exception as e:
                logging.error(f'An error occured: {e}')
                continue

if __name__ == "__main__":
    stream_data_json()



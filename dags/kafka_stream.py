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

    for pages in range(1,4):
        if time.time() > curr_time + 60:
            break
        try :
            data = links.get_data(pages, 50, '004', 2015, 'A')  # 데이터 가져오기, 이 부분은 데이터 구조에 맞게 수정 필요
            producer.send('book_json_data',data)
        except Exception as e :
            logging.error(f'An error occured: {e}')
            continue

if __name__ == "__main__":
    stream_data_json()



# library
from datetime import datetime
from kafka import KafkaProducer
import time
import logging
import json
import pandas as pd

# Modules
from config.parsing_request import Yes24Scraper
from config.requests_urls import collect_links

# logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def stream_data_json():

    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000,
        value_serializer= lambda x: json.dumps(x).encode('utf-8')
    )

    curr_time = time.time()
    url_data = pd.concat(collect_links())

    for idx, row in enumerate(url_data['links']):

        if time.time() > curr_time + 60:
            break

        try:

            data = Yes24Scraper(f"https://www.yes24.com{row[idx]}").get_all_info()
            print(f"현재 {idx}번째 순환중, {row} 링크 탐색중...")
            future = producer.send('book_product_json', data)
            future.get(timeout=10) # kafka 전송이 성공했는지 확인
            time.sleep(3)

        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue
        except IndexError as i:
            logging.error(f'An Index Errors{i}')
            pass
    producer.close()

if __name__ == "__main__":
    stream_data_json()



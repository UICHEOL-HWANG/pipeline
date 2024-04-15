# Module
from config.stroage_input import *

# library
from kafka import KafkaConsumer
import json
import pandas as pd
import logging
import requests
from urllib.parse import urlparse, unquote

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def consumer_to_image_gcp(max_messages=100):
    consumer = KafkaConsumer(
        'book_json_data',
        bootstrap_servers='broker:29092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True
    )

    data_lst = []
    message_count = 0
    try:
        while message_count < max_messages:
            msg_pack = consumer.poll(timeout_ms=5000)
            if not msg_pack:
                break
            for tp, messages in msg_pack.items():
                for message in messages:
                    tmp_df = pd.DataFrame([message.value])
                    data_lst.append(tmp_df)
    finally:
        consumer.close()

    return pd.concat(data_lst)



def upload_to_gcp():
    data = consumer_to_image_gcp()
    bucket_name = 'book_pipeline'

    for idx, row in data.iterrows():
        image_url = row['image']
        response = requests.get(image_url)
        # URL에서 파일 이름 추출
        parsed_url = urlparse(image_url)
        blob_name = f'years_image/{row["title"]}_{unquote(parsed_url.path.split("/")[-1])}'

        uploader = UploadToCloud(bucket_name, blob_name, response.content)
        try:
            uploader.upload_to_bytesfile()
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

if __name__ == "__main__":
    upload_to_gcp()
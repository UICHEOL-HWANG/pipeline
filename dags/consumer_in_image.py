# Module
from config.stroage_input import *

# library
from kafka import KafkaConsumer
import json
import pandas as pd
import logging
import requests


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
                    tmp_df = pd.DataFrame(message.value['data']['bestSeller'])
                    data_lst.append(tmp_df)
    finally:
        consumer.close()

    return pd.concat(data_lst)



def upload_to_gcp():
    data = consumer_to_image_gcp()

    buckect_name = 'book_pipeline'

    for idx,row in data.iterrows():
        image_url = f'https://contents.kyobobook.co.kr/sih/fit-in/300x0/pdt/{row["cmdtCode"]}.jpg'
        data = requests.get(image_url)
        blob_name = f'years_image/{row["cmdtCode"]}.jpg'
        uploader = UploadToCloud(buckect_name,blob_name,data.content)
        try:
          uploader.upload_to_bytesfile()
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


if __name__ == "__main__":
    upload_to_gcp()
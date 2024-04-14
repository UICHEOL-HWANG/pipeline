# Module
from config.stroage_input import *

# library
from kafka import KafkaConsumer
from datetime import datetime
import json
import logging
import time


def consumer_to_upload(max_messages=100):
    consumer = KafkaConsumer(
        'book_json_data',
        bootstrap_servers=['broker:29092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True
    )

    bucket_name = 'book_pipeline'
    message_count = 0

    try:
        while message_count < max_messages:
            msg_pack = consumer.poll(timeout_ms=5000)  # 5초 동안 기다린 후 반환
            if not msg_pack:
                break  # 메시지가 없으면 종료

            for tp, messages in msg_pack.items():
                for message in messages:
                    data = message.value
                    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                    blob_name = f"years_data/data_{timestamp}_{message_count+1}.json"
                    uploader = UploadToCloud(bucket_name, blob_name, data)
                    uploader.upload_json_to_gcs()
                    message_count += 1
                    if message_count >= max_messages:
                        break
    finally:
        consumer.close()

if __name__ == "__main__":
    consumer_to_upload()

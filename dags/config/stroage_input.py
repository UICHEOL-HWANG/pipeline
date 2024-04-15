# library

from google.cloud import storage
from google.oauth2 import service_account
import json
import io
from io import BytesIO

class UploadToCloud:
    """
    GCP upload용 클래스
    """
    def __init__(self, bucket_name : str, blob_name : str, data : dict):
        """
        :param bucket_name: 버킷네임 필수 지정 요망
        :param blob_name: 버킷 내에서 어떤 디렉토리 경로 / 파일이름으로 저장할지 선정
        :param data: 어떤 데이터를 집어 넣을 것인지?
        """
        self.bucket_name = bucket_name
        self.blob_name = blob_name
        self.data = data
        # on_premise 서버용 루트
        # self.key_pair = '/Users/uicheol_hwang/Data_Engineering/dags/config/access_key.json'
        self.key_pair = '/opt/airflow/dags/config/access_key.json' # Docker 서버용 루트
        credentials = service_account.Credentials.from_service_account_file(self.key_pair)
        self.client = storage.Client(credentials=credentials, project=credentials.project_id)
    def upload_json_to_gcs(self):
        """

        :return: Json 파싱용이니 참고
        """
        bucket = self.client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.blob_name)
        # JSON 데이터를 문자열로 변환하고 바이트 스트림으로 인코딩
        json_data = json.dumps(self.data)
        json_bytes = json_data.encode('utf-8')
        stream = io.BytesIO(json_bytes)
        blob.upload_from_file(stream, content_type='application/json')
        print(f"{self.blob_name} has been uploaded to {self.bucket_name}")
    def upload_to_bytesfile(self):

        """
        :return: 이미지 파일 업로드 때 사용
        """
        if not isinstance(self.data, bytes):
            raise ValueError("Data must be bytes.")
        # 예외 조건 이미지가 아닌 경우 raise 오류로 인한 처리

        bucket = self.client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.blob_name)
        data = BytesIO(self.data)
        blob.upload_from_file(data, content_type='image/jpeg')
        print(f"{self.blob_name} has been uploaded to {self.bucket_name}")
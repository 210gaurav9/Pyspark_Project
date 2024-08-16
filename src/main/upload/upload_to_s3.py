from resources.dev import config
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import *
import traceback
import datetime
import os

from src.main.utility.s3_client_object import S3ClientProvider


class UploadToS3:
    def __init__(self,s3_client):
        self.s3_client = s3_client

    def upload_to_s3(self,s3_directory,s3_bucket,local_file_path):
        current_epoch = int(datetime.datetime.now().timestamp()) * 1000
        s3_prefix = f"{s3_directory}/{current_epoch}/"
        try:
            for root, dirs, files in os.walk(local_file_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    s3_key = f"{s3_prefix}/{file}"
                    self.s3_client.upload_file(local_file_path, s3_bucket, s3_key)
            return f"Data Successfully uploaded in {s3_directory} data mart "
        except Exception as e:
            logger.error(f"Error uploading file : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e


# aws_access_key = config.aws_access_key
# aws_secret_key = config.aws_secret_key
# s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
# s3_client = s3_client_provider.get_client()
# upload = UploadToS3(s3_client)
# upload.upload_to_s3(config.s3_source_directory,config.bucket_name,config.local_directory)
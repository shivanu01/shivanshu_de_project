import os.path
import traceback

from src.main.utility.logging_config import logger


class S3FileDownloader:
    def __init__(self,s3_client,bucket_name,local_directory):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.local_directory = local_directory

    def download_files(self,file_paths):
        logger.info(f"Running downloads in the follwing file {file_paths}")
        for key in file_paths:
            file_name = os.path.basename(key)
            logger.info(f"file name is {file_name}")
            download_file_path = os.path.join(self.local_directory,file_name)

            try:
                self.s3_client.download_file(self.bucket_name,key,download_file_path)
            except Exception as e:
                error_message =f"Error downloading the file {key} : {str(e)}"
                traceback_message = traceback.format_exc()
                print(error_message)
                print(traceback_message)
                raise e

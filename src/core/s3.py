import os
import  s3fs
import json
import requests
import logging
import botocore.exceptions


from logging import  handlers
from dotenv import load_dotenv



load_dotenv()


class S3():
    def __init__(self):
        super().__init__()

    def check(self, file_path):
        client_kwargs = {
            'key': os.getenv('KEY'),
            'secret': os.getenv('SECRET_KEY'),
            'endpoint_url': os.getenv('ENDPOINT_URL'),
            'anon': False
        }
        s3 = s3fs.core.S3FileSystem(**client_kwargs)
        try:
            file_content = s3.cat(file_path)
            response = json.loads(file_content)
            return response
        except botocore.exceptions.ClientError as e:
            print(f"File not found: {e}")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def send_json_s3_v2(self, metadata, path_data_raw):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [ %(levelname)s ] :: %(message)s',
                            datefmt="%Y-%m-%dT%H:%M:%S", handlers=[
                handlers.RotatingFileHandler(f's3_debug.log'),
                logging.StreamHandler()
            ])
        client_kwargs = {
            'key': os.getenv('KEY'),
            'secret': os.getenv('SECRET_KEY'),
            'endpoint_url': os.getenv('ENDPOINT_URL'),
            'anon': False
        }
        s3 = s3fs.core.S3FileSystem(**client_kwargs)
        json_s3 = str(path_data_raw)
        json_data = json.dumps(metadata, indent=4, ensure_ascii=False)
        try:
            with s3.open(json_s3, 'w') as s3_file:
                s3_file.write(json_data)
            logging.info(f'File {json_s3} berhasil diupload ke S3.')
        except Exception as e:
            logging.error(f'Gagal mengunggah file {json_s3} ke S3: {e}')
            logging.info(f'File {json_s3} gagal diupload ke S3.')

    def download(self, link, path_data):
        client_kwargs = {
            'key': os.getenv('KEY'),
            'secret': os.getenv('SECRET_KEY'),
            'endpoint_url': os.getenv('ENDPOINT_URL'),
            'anon': False
        }
        s3 = s3fs.core.S3FileSystem(**client_kwargs)
        pdf_s3 = path_data
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(link, headers=headers, verify=False)
        if response.status_code == 200:
            with s3.open(os.path.join(pdf_s3), "wb") as file:
                file.write(response.content)
            logging.info(f"File successfully saved in {pdf_s3}.")
            logging.info(f'File {link} berhasil diupload ke S3.')
        else:
            print(f"Failed to download file {link}. Status Code: {response.status_code}")


    def read_file(self, path_data):
        client_kwargs = {
                'key': os.getenv('KEY'),
                'secret': os.getenv('SECRET_KEY'),
                'endpoint_url': os.getenv('ENDPOINT_URL'),
                'anon': False
            }
        s3 = s3fs.core.S3FileSystem(**client_kwargs)

        # Tentukan path folder di S3
        folder_path = path_data

        # Dapatkan daftar file dalam folder
        files = s3.ls(folder_path)
        print(len(files))

        logging.info(f'file upload {len(files)} in {path_data}')


        datas = {}
        # Tampilkan daftar file
        for i,file_path in enumerate(files, start=1):
            datas.update({str(i):file_path})


        with open('../../datas.json', 'w') as file:
            json.dump(datas, file, indent=4)


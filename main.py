import json
import logging
import time

from datetime import datetime
from logging import handlers

from src.helper.hardcode import HardCode


sekarang = datetime.now()
format_ymd_hms = sekarang.strftime("%Y-%m-%d %H:%M:%S")


logging.basicConfig(level=logging.INFO, format='%(asctime)s [ %(levelname)s ] :: %(message)s',
                    datefmt="%Y-%m-%dT%H:%M:%S", handlers=[
        handlers.RotatingFileHandler(f'debug.log'),
        logging.StreamHandler()
    ])


class Main:
    def __init__(self):
        self.hardcode = HardCode()

    def _main(self, url):
        result = self.hardcode.processing_data(self.hardcode.get_response(url))

        for link, sub_title in zip(result['link'], result['sub_title']):
            response_detail = self.hardcode.get_response('https://katalogdata.kemenparekraf.go.id/' + link)
            detail = self.hardcode.detail(response_detail)

            metadata = dict(
                link = 'https://katalogdata.kemenparekraf.go.id/' + link,
                tags = [
                    'katalogdata'
                ],
                domain = 'katalogdata.kemenparekraf.go.id',
                title = result['desc']['title'],
                sub_title = sub_title,
                description = result['desc']['description'],
                additional_info = result['desc']['additional_info'],
                tags_info = result['desc']['tags_info'],
                other_infomation = detail,
                path_data_raw = [
                    f's3://ai-pipeline-statistics/data/data_raw/data statistic/satu data Kementerian Pariwisata dan Ekonomi Kreatif/Data statistik/json/{detail["link_download"].split("/")[-1].split(".")[0]}.json',
                    f's3://ai-pipeline-statistics/data/data_raw/data statistic/satu data Kementerian Pariwisata dan Ekonomi Kreatif/Data statistik/{detail["link_download"].split("/")[-1].split(".")[-1].lower()}/{detail["link_download"].split("/")[-1]}',
                ],
                path_data_clean=[
                    f's3://ai-pipeline-statistics/data/data_clean/data statistic/satu data Kementerian Pariwisata dan Ekonomi Kreatif/Data statistik/json/{detail["link_download"].split("/")[-1].split(".")[0]}.json',
                    f's3://ai-pipeline-statistics/data/data_clean/data statistic/satu data Kementerian Pariwisata dan Ekonomi Kreatif/Data statistik/{detail["link_download"].split("/")[-1].split(".")[-1].lower()}/{detail["link_download"].split("/")[-1]}',
                ],
                crawling_time = format_ymd_hms,
                crawling_time_epoch = int(time.time())
            )

            print(metadata)

            # try:
            #     self.hardcode.download(link=detail['link_download'], path_data=metadata['path_data_raw'][1])
            #     self.hardcode.send_json_s3_v2(metadata=metadata, path_data_raw=metadata['path_data_raw'][0])
            #     logging.info(metadata['path_data_raw'])
            # except:
            #     logging.error(sub_title)
            #     continue

    def _get_link(self, url):
        response = self.hardcode.get_response(url)
        return self.hardcode.all_source(response)


if __name__ == "__main__":
    main = Main()
    for i in range(0, 40, 10):
        source = main._get_link(url=f'https://satudata.kemenparekraf.go.id/search?q=&from={i}')
        for url in source:
            main._main(
                url = url
            )
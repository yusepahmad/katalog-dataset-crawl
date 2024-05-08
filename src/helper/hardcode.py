from bs4 import BeautifulSoup
import requests

from ..utils.parser import ParserHTML

class HardCode(ParserHTML):
    def __init__(self) -> None:
        self.url = ''
        self.response_text = ''
        self.parser = ParserHTML()

    def get_response(self, url):
        self.response_text = requests.get(url).text
        soup = BeautifulSoup(self.response_text, 'html.parser')
        return soup

    def processing_data(self, soup):
        return dict(
            link = self.parser_link(soup),
            sub_title = self.parser_title(soup),
            desc = self.default_content(soup)
        )

    def detail(self, soup):
        return self.result_detail(soup)

    def all_source(self, soup):
        content = soup.find(class_='results')
        return [link.find('a').get('href') for link in content.find_all(class_='col-sm-8 pad-xs-left-zero pad-xs-right-zero') if 'https://katalogdata.kemenparekraf.go.id/' in link.find('a').get('href')]

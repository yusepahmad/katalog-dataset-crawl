from ..core.s3 import S3

class ParserHTML(S3):
    def __init__(self):
        super().__init__()

    def parser_link(self, soup):
        return list({link.find('a').get('href') for link in soup.find_all(class_='resource-item')})

    def parser_title(self, soup):
        return list({link.find('a').get('title') for link in soup.find_all(class_="resource-item")})

    def default_content(self, soup):
        return dict(
            title = soup.find(class_='primary col-sm-9 col-xs-12').find('h1').text,
            description = soup.find(class_='notes embedded-content').text,
            additional_info = {key.text: value.text for tr in soup.find(class_='additional-info').find_all('tr') for key, value in zip(tr.find_all('th'), tr.find_all('td'))},
            tags_info = [li.text.replace('\n', '') for li in soup.find(class_='tags').find_all('li')]
    )

    def result_detail(self, soup):
        return {'link_download': soup.find(class_='resource-url-analytics').get('href'), **{trs.find('th').text: trs.find('td').text for tbl in soup.find_all('tbody') for tr in tbl.find_all('tr') for trs in [tr] if tr.find('td') and tr.find('th')}}


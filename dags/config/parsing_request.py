from typing import List
import requests
from lxml import html


class Yes24Scraper:
    """
    default detail page 추출
    """
    def __init__(self, url):
        """

        :param url: 일전 requsts에서 받아온 데이터를 기반으로 enumerate loop를 통해 반복 예정
        """
        self.url = url
        self.response = requests.get(self.url)
        self.tree = html.fromstring(self.response.text)
        self.data = self.get_all_info()
        
    def get_all_info(self):
        title = self.tree.xpath("//div[@class='gd_titArea']//h2[@class='gd_name']/text()")[0]
        authors = self.tree.xpath("//span[@class='gd_pubArea']//span[@class='gd_auth']//a/text()")[0]
        publisher = self.tree.xpath("//span[@class='gd_pub']//a/text()")[0]
        publish_date = self.tree.xpath("//span[@class='gd_date']/text()")[0]
        rating = self.tree.xpath("//span[@id='spanGdRating']//a//em[@class='yes_b']/text()")[0]
        image = self.tree.xpath("//img[@class='gImg']/@src")[0]
        price = self.tree.xpath("//span[@class='nor_price']//em/text()")[0]
        related_keywords = self.tree.xpath("//ul[@class='yesAlertLi']//li//a/text()")[0]
        description = self.tree.xpath("//div[@class='infoWrap_txtInner']//textarea/text()")

        return {
            'title': title,
            'authors': authors,
            'publisher': publisher,
            'publish_date': publish_date,
            'rating': rating,
            'image': image,
            'price': price,
            'related_keywords': related_keywords,
            'description': description
        }

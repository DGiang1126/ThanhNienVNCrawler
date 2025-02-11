import scrapy
from test_crawler_2.items import TestCrawler2Item

class ThanhnienvnSpider(scrapy.Spider):
    name = "ThanhnienVN"
    allowed_domains = ["thanhnien.vn"]
    start_urls = ["https://thanhnien.vn/"]

    def parse(self, response):
        news = response.xpath('.//div[@class="item-sub"]/div[@class="box-category-item"]/a')

        for new in news:
            item = TestCrawler2Item()

            item['title'] = new.xpath('@title').get()

            new_details_url = new.xpath("@href").get()
            if new_details_url:
                new_details_url = response.urljoin(new_details_url)
                yield scrapy.Request(url=new_details_url, callback=self.parse_news, meta={'item': item})
    # https://thanhnien.vn/thuc-hu-thong-tin-dung-xe-mac-ao-mua-bi-phat-14-trieu-dong-185250208155556985.htm
    def parse_news(self,response):
        item = response.meta['item']

        item['author'] = response.xpath('.//a[@class="name"]/text()').get()
        item['gmail'] = ''.join(response.xpath('.//span[@class="email"]/text()').getall()).strip()
        item['time'] = ''.join(response.xpath('.//div[@class="detail-time"]/div/text()').getall()).strip()
        item['content'] = ''.join(response.xpath('.//h2[contains(@class, "detail-sapo")]//text()').getall()).strip()
        

        yield item




    

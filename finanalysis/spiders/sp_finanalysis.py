import scrapy
import json
import tushare as ts
from finanalysis.items import EPSItem


class SpFinanalysis(scrapy.Spider):
    name = "sp_finanalysis"
    allowed_domains = ["gw.datayes.com"]
    start_urls = ["https://r.datayes.com/"]
    root_url = "https://gw.datayes.com/rrp_adventure/fdmtNew/{}?reportType=SUMMARY&reportPeriodType=A&duration=ACCUMULATE&includeLatest=true&period=10"

    def __init__(self):
        pass

    def parse(self, response):
        cookies = {'cloud-sso-token': '3BC1A02286513D808BE7F665214811BF'}
        url = self.root_url.format("002797")
        yield scrapy.Request(url, cookies=cookies, callback=self.parse_ticker, cb_kwargs={"symbol": "002797"})

    def parse_ticker(self, response, symbol):
        body = response.body.decode('utf-8')
        json_object = json.loads(body)
        data = json_object["data"]
        dataRows = data["dataRow"]
        for dataRow in dataRows:
            code = dataRow["code"]
            # EPS(基本)
            if code == "EPSJB":
                item = self.convert_eps_item(symbol, "jb", dataRow)
                yield item
            # EPS(稀释)
            if code == "EPSXS":
                item = self.convert_eps_item(symbol, "xs", dataRow)
                yield item
            # EPS(摊薄)
            if code == "EPSTB":
                item = self.convert_eps_item(symbol, "tb", dataRow)
                yield item

    def convert_eps_item(self, symbol, type, dataRow):
        dataRow_data_items = dataRow["data"]
        item = EPSItem()
        item["symbol"] = symbol
        item["type"] = type
        for i in range(11):
            eps_jb = dataRow_data_items[i]
            item["s" + str(i)] = eps_jb
        return item

import scrapy
import json
import tushare as ts
from finanalysis.items import EPSItem
from finanalysis.items import ROEItem
import finanalysis.settings as settings


class SpFinanalysis(scrapy.Spider):
    name = "sp_finanalysis"
    allowed_domains = ["gw.datayes.com"]
    start_urls = ["https://r.datayes.com/"]
    root_url = "https://gw.datayes.com/rrp_adventure/fdmtNew/{}?reportType=SUMMARY&reportPeriodType=A&duration=ACCUMULATE&includeLatest=true&period=10"

    def __init__(self):
        pass

    def parse(self, response):
        # 获取 股票列表
        ts.set_token(settings.TUSHARE_TOKEN)
        pro = ts.pro_api()
        data = pro.stock_basic(exchange='', list_status='L', fields='symbol')
        for symbol in data["symbol"]:
            cookies = {'cloud-sso-token': settings.CLOUD_SSO_TOKEN}
            url = self.root_url.format(symbol)
            yield scrapy.Request(url, cookies=cookies, callback=self.parse_ticker, cb_kwargs={"symbol": symbol})

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

            # ROE(摊薄)
            if code == "ROETB":
                item = self.convert_roe_item(symbol, "tb", dataRow)
                yield item

            # ROE(加权平均)
            if code == "ROEJJPJ":
                item = self.convert_roe_item(symbol, "jjpj", dataRow)
                yield item

    def convert_eps_item(self, symbol, type, dataRow):
        dataRow_data_items = dataRow["data"]
        item = EPSItem()
        item["symbol"] = symbol
        item["type"] = type

        # 空值
        for i in range(11):
            item["s" + str(i)] = None

        l = len(dataRow_data_items)
        for i in range(l):
            eps_jb = dataRow_data_items[i]
            item["s" + str(i)] = eps_jb
        return item

    def convert_roe_item(self, symbol, type, dataRow):
        dataRow_data_items = dataRow["data"]
        item = ROEItem()
        item["symbol"] = symbol
        item["type"] = type

        # 空值
        for i in range(11):
            item["s" + str(i)] = None

        l = len(dataRow_data_items)
        for i in range(l):
            eps_jb = dataRow_data_items[i]
            item["s" + str(i)] = eps_jb
        return item
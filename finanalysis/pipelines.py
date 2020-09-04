# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
import finanalysis.settings as settings
import pymysql
import finanalysis.items as items


class JsonWriterPipeline:
    def __init__(self):
        # 参数初始化，可选实现
        self.file = open(settings.ROOT_PATH + "eps.json", 'wb')

    def process_item(self, item, spider):
        line = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(line.encode('utf-8'))
        self.file.flush()
        return item

    def close_spider(self, spider):
        # 可选实现，当spider被关闭时，这个方法被调用
        self.file.close()


class MysqlWriterPipeline:
    def __init__(self):
        # 打开数据库连接
        self.db = pymysql.connect(host=settings.DB_SERVER_NAME, user=settings.DB_SERVER_USER_NAME,
                                  password=settings.DB_SERVER_PASSWORD, db=settings.DB_NAME, charset="utf8")

        # 使用cursor()方法获取操作游标
        cursor = self.db.cursor()
        # 执行sql语句
        cursor.execute("truncate roe")
        cursor.execute("truncate eps")
        # 提交到数据库执行
        self.db.commit()

    def process_item(self, item, spider):
        table = ""

        if type(item) == items.ROEItem:
            table = "roe"

        if type(item) == items.EPSItem:
            table = "eps"

        # 使用cursor()方法获取操作游标
        cursor = self.db.cursor()

        # SQL 插入语句
        sql = """ INSERT INTO `stock`.`{}` (`symbol`, `type`, `s0`, `s1`, `s2`, `s3`,  `s4`, `s5`, `s6`, `s7`, `s8`, `s9`, `s10`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        sql = sql.format(table)

        # 执行sql语句
        cursor.execute(sql, (item["symbol"], item["type"], item["s0"], item["s1"], item["s2"],
                             item["s3"], item["s4"], item["s5"], item["s6"], item["s7"], item["s8"], item["s9"],
                             item["s10"]))
        # 提交到数据库执行
        self.db.commit()

        return item

    def close_spider(self, spider):
        # 关闭数据库连接
        self.db.close()

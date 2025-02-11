# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import json
import csv
import mysql.connector
import psycopg2

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class MongoDBThanhNienVNPipeline:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["ThanhNienVN"]
        self.collection = self.db["dbthanhvienvn"]
        pass

    def process_item(self, item, spider):
        try:
            self.collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item: {e}")
    
    def close_spider(self, spider):
        self.client.close()  # Đóng kết nối MongoDB

class JsonDBThanhNienVNPipeline:
    def __init__(self):
        self.data = []

    def open_spider(self, spider):
        self.file = open("D:\\test_crawler_2\\test_crawler_2\data\\json\\thanhvienvn.json", "w", encoding="utf-8")

    def process_item(self, item, spider):
        self.data.append(dict(item))
        return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, ensure_ascii=False, indent=4)
        self.file.close()

class CSVThanhNienVNPipeline:
    def __init__(self):
        self.file = open("D:\\test_crawler_2\\test_crawler_2\data\\csv\\thanhnienvn.csv", "a", newline="", encoding="utf-8")
        self.csv_writer = csv.writer(self.file, delimiter="$")
        self.csv_writer.writerow(["title", "author","gmail", "time", "content"])  # Ghi header chỉ 1 lần

    def process_item(self, item, spider):
        title = item.get("title", "Không có")
        author = item.get("author", "Không có")
        gmail = item.get("gmail", "Không có")
        time = item.get("time", "Không có")
        content = item.get("content", "Không có")
        self.csv_writer.writerow([title, author, gmail, time, content])  # Chuyển thành list
        return item

    def close_spider(self, spider):
        self.file.close()

class TxtThanhNienVNPipline:
    def open_spider(self, spider):
        self.file = open("D:\\test_crawler_2\\test_crawler_2\\data\\txt\\thanhnienvn.txt", "w", encoding="utf-8")

    def process_item(self, item, spider):
        title = item.get("title", "Không rõ ")
        author = item.get("author", "Không rõ")
        gmail = item.get("gmail", "Chưa có")
        time = item.get("time", "Không rõ")
        content = item.get("content", "Không rõ")

        line = f"Tiêu đề: {title} | Tác giả: {author} | Gmail: {gmail} | Thời gian: {time} | Nội dung: {content}\n"

        self.file.write(line)
        return item
    def close_spider(self, spider):
        self.file.close()

class MySQLThanhNienVNPipline:
    def __init__(self):
        self.connect = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = "12345678",
            database = "thanhvienvndb"
        )
        self.cursor = self.connect.cursor()
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS thanhnienvn_crawler(
            id INT NOT NULL AUTO_INCREMENT,
            title TEXT,
            author TEXT,
            gmail TEXT,
            time TEXT,
            content TEXT,
            PRIMARY KEY(id)                
        )
        """)
    
    def process_item(self, item, spider):
        self.cursor.execute('''
            INSERT INTO thanhnienvn_crawler(title, author, gmail, time, content)
            VALUES(%s, %s, %s, %s, %s)
                            ''',(
            item['title'],
            item['author'],
            item['gmail'],
            item['time'],                    
            item['content']                    
                            ))
        self.connect.commit()
        return item
    
    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()

class PostgresThanhNienVNPipeline:
    def __init__(self):
        self.connect = psycopg2.connect(
            host='localhost',
            user='postgres',
            password='12345678',
            database='dbthanhvienvn'
        )
        self.cursor = self.connect.cursor()
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS thanhnienvn (
            id SERIAL PRIMARY KEY,
            title TEXT,
            author TEXT,
            gmail TEXT,
            time TEXT,
            content TEXT
        )
        """)
        self.connect.commit()

    def process_item(self, item, spider):
        self.cursor.execute('''
            INSERT INTO thanhnienvn (title, author, gmail, time, content)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            item.get('title', ''),
            item.get('author', ''),
            item.get('gmail', ''),
            item.get('time', ''),
            item.get('content', '')
        ))
        self.connect.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()


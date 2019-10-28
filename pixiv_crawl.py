import requests
import time
import re
import json
import os
import random
import pymysql
import pandas as pd
from scrapy import Request,Spider
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
from urllib.parse import urlencode
from selenium import webdriver
import browsercookie
now = time.localtime()
date = datetime(year=now.tm_year, month=now.tm_mon, day=now.tm_mday) - timedelta(days=3)
subtract = timedelta(days=2)


headers = {
'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0',
}

class crawl(object):


    def __init__(self):
        self.host = 'localhost'
        self.user = 'root'
        self.password = '123456'
        self.port = 3306

    def start_requests(self):
        params = {'mode': 'monthly',
                  'content': 'illust',
                  }
        base_url = "https://www.pixiv.net/ranking.php?"
        for i in range(0, 500):
            crawl_date = date - i*subtract
            self.date_str = crawl_date.strftime('%Y%m%d')
            params['date'] = self.date_str
            # for page in [1]:
            params['p'] = 1
            url = base_url + urlencode(params)
            yield crawl_date, url

    def get_one_page(self, url):
        count = 0
        try:
            response = requests.get(url, headers=headers)
            a = response.status_code
            if a == 200:
                return response.text
        except requests.ConnectionError as e:
            while True:
                count += 1
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        return response.text
                except requests.ConnectionError as e:
                    time.sleep(10)
                    print('Retry: {}'.format(count))


    def parse_requests(self):
        exist_pic = [f.split('_p0')[0] for f in os.listdir('./images')]
        pic_set = set(exist_pic)
        del exist_pic
        for i, (date, url) in enumerate(self.start_requests()):
            html = self.get_one_page(url)
            soup = BeautifulSoup(html, 'lxml')
            illusts_html = soup.find_all(name='section', attrs={'class': 'ranking-item'})
            for ill in illusts_html:
                master_url = ill.img.attrs['data-src']
                middle = master_url.split('master', 1)[1]
                middle2 = middle.split('_p0')[0]
                pic_url_png = 'https://i.pximg.net/img-original{}_p0.png'.format(middle2)
                pic_url_jpg = 'https://i.pximg.net/img-original{}_p0.jpg'.format(middle2)
                rank = ill.attrs['id']
                date = ill.attrs['data-date']
                title = ill.attrs['data-title']
                id = ill.attrs['data-id']
                referer = 'https://www.pixiv.net' + ill.find(name='a', attrs={'target': '_blank'}).attrs['href']
                headers['Referer'] = referer
                if id in pic_set:
                    continue
                for pic in [pic_url_jpg, pic_url_png]:
                    try:
                        response = requests.get(pic, headers=headers, timeout=200)
                    except:
                        print('请求失败，清等待……')
                        time.sleep(10)
                        print('等待完毕。')
                        continue
                    if response.content is None:
                        print('网络断了。')
                        time.sleep(20)
                        print('尝试下一次请求')
                        continue
                    elif response.status_code != 200:
                        continue
                    elif response.status_code == 200:
                        content = response.content
                        pic_path = './images2/{}'.format(pic.split('/')[-1])
                        path = os.path.abspath(pic_path)
                        with open(pic_path, 'wb') as f:
                            f.write(content)
                        self.insert_data(rank, date, title, id, path)
                        print('{} - {} saved success!'.format(date, title))
                        pic_set.add(id)
                        break
                    # except requests.ConnectionError as e:
                    #     while True:
                    #         try:
                    #             response = requests.get(pic, headers=headers)
                    #             if response.status_code == 200:
                    #                 content = response.content
                    #                 pic_path = './images/{}'.format(pic.split('/')[-1])
                    #                 path = os.path.abspath(pic_path)
                    #                 with open(pic_path, 'wb') as f:
                    #                     f.write(content)
                    #                 print('{} saved success!'.format(title))
                    #                 self.insert_data(rank, date, title, id, path)
                    #                 break
                    #         except requests.ConnectionError as e:
                    #             time.sleep(10)
                    #             count += 1
                    #             print('Retry to download image: {} {}'.format(count, pic))
                time.sleep(random.randint(10, 15))

    def _init_mysql(self):
        host, user, password, port = (self.host, self.user, self.password, self.port)
        db = pymysql.connect(host=host, user=user, password=password, port=port)
        cursor = db.cursor()
        exist = 'show databases;'
        cursor.execute(exist)
        all_base = list(cursor.fetchall())
        if ('Pixiv',) in all_base:
            pass
        else:
            database = 'CREATE DATABASE Pixiv DEFAULT CHARACTER SET utf8'
            cursor.execute(database)
        db.close()

    def create_table(self):
        host, user, password, port = (self.host, self.user, self.password, self.port)
        self.db = pymysql.connect(host=host, user=user, password=password,
                             port=port, db='Pixiv', use_unicode=True)
        cursor = self.db.cursor()
        exist = 'show tables;'
        cursor.execute(exist)
        all_table = list(cursor.fetchall())
        if ('picture',) in all_table:
            pass
        else:
            table = 'CREATE TABLE IF NOT EXISTS picture (rank FLOAT NOT NULL,post_date VARCHAR(255) NOT NULL,' \
                    'title VARCHAR(255) NOT NULL,id VARCHAR(255) NOT NULL,path VARCHAR(255) NOT NULL,' \
                    'crawl_date DATE NOT NULL,PRIMARY KEY (id))'
            cursor.execute(table)

    def insert_data(self, rank, date, title, pic_id, path):
        cursor = self.db.cursor()
        if not isinstance(rank, int):
            rank = int(rank)
        if not isinstance(title, str):
            title = 'NULL'
        if not isinstance(date, str):
            date = 'NULL'
        data_format = 'INSERT INTO picture (rank,post_date,title,id,path,crawl_date) ' \
                          'values (%s,%s,%s,%s,%s,%s)'
        try:
            cursor.execute(data_format, (rank, date, title, pic_id, path, self.date_str))
            self.db.commit()
            print('success saved in database {}'.format(title))
        except pymysql.Error as e:
            print('Error: {}'.format(e.args))
            self.db.rollback()

    def run(self):
        self._init_mysql()
        self.create_table()
        self.parse_requests()

if __name__ == '__main__'
cw = crawl()
cw.run()













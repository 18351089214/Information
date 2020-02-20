# -*- coding: utf-8 -*-
import random
import os
import sys
import time
import platform
from datetime import datetime
from urllib.parse import quote
from urllib.parse import urlencode

import requests
from gtaasmysql import MySQL
from loggtaas import Log
from gtaasutils import getHeaders
from information.config import *
from lxml import etree

requests.packages.urllib3.disable_warnings()


class Weibo(object):
    # 类初始化
    def __init__(self):
        super(Weibo, self).__init__()
        name = os.path.split(__file__)[-1].split(".")[0]
        log_path = os.path.join(os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep), 'log')
        if platform.platform().startswith('Windows'):
            self.log = Log(name=name, path=log_path + '\\' + name + r".log", level='ERROR')
        else:
            self.log = Log(name=name, path=log_path + '/' + name + r".log", level='ERROR')
        self.logger = self.log.Logger
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        self.obj_mysql = MySQL(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_DATABASE)
        self.obj_mysql.create(SQL_CREATE_WEIBO_TABLE)
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    def get_cookies(self):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        try:
            resp = requests.get(WEIBO_COOKIE_POOL_URL)
            if resp.status_code == 200:
                cookie = eval(str(resp.content, encoding="utf8"))
                self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
                return cookie
            return None
        except Exception as e:
            print(e.args)
            self.logger.error('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
            return None

    def get_page(self, offset, keyword):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        params = {
            'topnav': '1',
            'wvr': '6',
            'b': '1',
            'page': str(offset)
        }
        try:
            self.base_url = 'https://s.weibo.com/weibo/' + quote(keyword) + '?'
            print('Crawling: ', self.base_url + urlencode(params))
            self.logger.debug('Crawling' + self.base_url + urlencode(params))
            response = requests.get(self.base_url, headers=getHeaders(),
                                    params=urlencode(params), verify=False, cookies=self.get_cookies())
            if response.status_code == 200:
                self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
                return response.text
            return None
        except Exception as e:
            print(e.args)
            self.logger.error('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
            return None

    def parse_page(self, resposne):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        try:
            if resposne:
                html = etree.HTML(resposne)
                items = html.xpath(
                    '//div[@id="pl_feed_main"]/div[@class="m-wrap"]/div[@id="pl_feedlist_index"]/div[1]/div[@class="card-wrap"]')
                for item in items:
                    try:
                        part_url = item.xpath(
                            './div[@class="card"]/div[@class="card-feed"]/div[@class="content"]/p[@class="from"]/a[1]/@href')[
                            0]
                        result = {}
                        result['mid'] = item.xpath('./@mid')[0]
                        result['url'] = 'https:' + part_url
                        user_id = \
                            item.xpath('./div[@class="card"]/div[@class="card-feed"]/div[@class="avator"]/a/@href')[0]
                        user_id = user_id.split('/')[-1]
                        result['id'] = user_id.split('?')[0]
                        result['title'] = ''.join(item.xpath(
                            './div[@class="card"]/div[@class="card-feed"]/div[@class="content"]/p[last() -1]/text()'))
                        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
                        yield result
                    except:
                        yield {}
        except Exception as e:
            print(e.args)
            self.logger.error('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
            yield {}

    def get_params(self, result: dict):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        try:
            url = 'https://m.weibo.cn/api/comments/show?id={id}'.format(id=result['mid'])
            time.sleep(random.random())
            response = requests.get(url=url, headers=getHeaders(), verify=False,
                                    cookies=self.get_cookies())
            if response.status_code == 200:
                data = response.json()
                d = data.get('data', None)
                result['comment_url'] = None
                if d:
                    max = d.get('max', None)
                    for page in range(1, max + 1):
                        result['comment_url'] = 'https://m.weibo.cn/api/comments/show?id={id}&page={page}'.format(
                            id=result['mid'], page=page)
                        yield result
                else:
                    yield result
                self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
        except Exception as e:
            print(e.args)
            self.logger.error('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
            yield {}

    def get_comments(self, keyword, result: dict):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        if result:
            data = {}
            data['url'] = result['url']
            data['keyword'] = keyword
            data['source'] = 'weibo'
            data['id'] = result['id']
            data['title'] = result['title']
            data['crawl_date'] = datetime.now().strftime('%Y-%m-%d')
            try:
                data['comment'] = ''
                if result['comment_url'] is None:
                    yield data
                else:
                    self.logger.debug(result['comment_url'])
                    time.sleep(random.random())
                    response = requests.get(url=result['comment_url'],
                                            headers=getHeaders(), verify=False, cookies=self.get_cookies())
                    if response.status_code == 200:
                        try:
                            json_data = response.json()
                            json_data = json_data.get('data', None)
                            if json_data is None:
                                yield data
                            else:
                                json_data = json_data.get('data', None)
                                for item in json_data:
                                    data['id'] = item['user']['id']
                                    data['comment'] = item['text'].strip()
                                    yield data
                            self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
                        except Exception as e:
                            print(e.args)
                            self.logger.error('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
                            yield data
            except requests.RequestException as e:
                self.logger.error('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
                yield {}

    # 调度函数
    def run(self):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        for keyword in KEYWORDS:
            for offset in range(WEIBO_BEGIN_OFFSET, WEIBO_END_OFFSET + 1):
                time.sleep(random.random() * 3)
                for item in self.parse_page(self.get_page(offset, keyword)):
                    time.sleep(random.random() * 2)
                    for item2 in self.get_params(dict(item)):
                        time.sleep(random.random() * 2)
                        for item3 in self.get_comments(keyword, dict(item2)):
                            self.obj_mysql.insert(item3, MYSQL_TABLE_WEIBO)
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

# -*- coding: utf-8 -*-
import os
import random
import re
import sys
import time
import platform
from urllib.parse import urlencode
from urllib.parse import urljoin

import requests
from information.config import *
from gtaasmysql import MySQL
from loggtaas import Log
from lxml import etree

from gtaasutils import getHeaders


class Pojie(object):
    def __init__(self):
        name = os.path.split(__file__)[-1].split(".")[0]
        log_path = os.path.join(os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep), 'log')
        if platform.platform().startswith('Windows'):
            self.log = Log(name=name, path=log_path + '\\' + name + r".log", level='DEBUG')
        else:
            self.log = Log(name=name, path=log_path + '/' + name + r".log", level='DEBUG')
        self.logger = self.log.Logger
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        self.obj_mysql = MySQL(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_DATABASE)
        self.obj_mysql.create(SQL_CREATE_POJIE_TABLE)
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    def get_page(self, offset, keyword):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        params = {
            'q': keyword,
            'p': str(offset),
            # 'srt': 'lds',
            'stp': '1',
            'cc': '52pojie.cn'
        }

        url = 'http://zhannei.baidu.com/cse/site?' + urlencode(params)
        self.logger.debug('Crawling: ' + url)
        try:
            response = requests.get(url=url, headers=getHeaders())
            if response.status_code == 200:
                self.logger.debug('Succeeded Crawl: ' + url)
                return response.text
            return None
        except:
            self.logger.error('Failed to crawl: ' + url)
            return None
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    # 解析一级页面内容，提取详情页URL
    def parse_page(self, response):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        if response:
            html = etree.HTML(response)
            items = html.xpath('//div[@id="results"]/div[contains(@class, "result")]')
            for item in items:
                url = item.xpath('./h3[@class="c-title"]/a/@href')[0]
                yield url
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    # 请求详情页
    def get_sub_page(self, url):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        time.sleep(random.random() * 2)
        try:
            response = requests.get(url=url, headers=getHeaders())
            if response.status_code == 200:
                self.logger.debug('Succeeded to crawl: ' + url)
                return response.text
            return None
        except:
            self.logger.error('Failed to crawl: ' + url)
            return None
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    def parse_sub_page(self, response, url):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        if response:
            html = etree.HTML(response)
            # 获取总页数
            try:
                total_str = html.xpath('//div[@id="pgt"]/div[@class="pgt"]/div[@class="pg"]/label/span/text()')[0]
                pattern = re.compile(r'.*?(\d+).*')
                total = int(re.search(pattern, total_str).group(1))
            except:
                # 异常则表示只有一页
                total = 1
            # 解析tid，用以构造url
            if url.find('thread-') != -1:
                tid = (url.split('thread-')[-1]).split('-')[0]
            elif url.find('tid=') != -1:
                url = url.split('tid=')[-1]
                pattern2 = re.compile(r'(\d+).*')
                tid = re.search(pattern2, url).group(1)

            for i in range(total):
                url = 'https://www.52pojie.cn/thread-{tid}-{offset}-1.html'.format(tid=tid, offset=str(i))
                try:
                    self.logger.debug('Crawling: %s' % url)
                    response = requests.get(url=url, headers=getHeaders())
                    result = {}
                    if response.status_code == 200:
                        self.logger.debug('Succeeded to crawl: %s' % url)
                        result = {'response': response.text, 'url': url}
                        yield result
                    yield result
                except:
                    self.logger.error('Failed to crawl: ' + url)
                    yield result
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    # 提取总页数并构造详情页URL
    def parse_detail(self, response, url, keyword):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        if response:
            try:
                html = etree.HTML(response)
                items = html.xpath('//div[@id="postlist"]/div[contains(@id, "post_")]')
                title = html.xpath('//span[@id="thread_subject"]/text()')[0]
                for item in items:
                    try:
                        useri = item.xpath('.//div[contains(@id, "favatar")]/div[1]/div/a/@href')[0]
                        user_id = useri.split('=')[-1]
                        result = {}
                        result['id'] = user_id
                        result['keyword'] = keyword
                        result['source'] = '52pojie'
                        result['title'] = title
                        result['user_name'] = ''.join(
                            item.xpath('.//div[contains(@id, "favatar")]/div[1]/div/a/text()'))
                        result['user_homepage'] = urljoin(POJIE_BASE_URL, useri)
                        result['post_on'] = (''.join(item.xpath('.//em[contains(@id, "authorposton")]/text()'))).strip()
                        result['content'] = (''.join(item.xpath('.//td[contains(@id, "postmessage")]/text()'))).strip()
                        if result['content'] is None:
                            result['content'] = ''
                        result['url'] = url
                        yield result
                    except:
                        self.logger.error('Failed to parse result')
                        continue
            except:
                self.logger.error('Failed to parse items or title')
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    def run(self):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        for keyword in KEYWORDS:
            for offset in range(POJIE_GROUP_START, POJIE_GROUP_END + 1):
                time.sleep(random.random() * 2)
                for url in self.parse_page(self.get_page(offset, keyword)):
                    time.sleep(random.random() * 2)
                    for result in self.parse_sub_page(self.get_sub_page(url), url):
                        if result.get('response', None) is not None:
                            for item in self.parse_detail(result.get('response', None), result.get('url', None),
                                                          keyword):
                                self.obj_mysql.insert(item, MYSQL_TABLE_POJIE)
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    def __del__(self):
        self.obj_mysql.close()

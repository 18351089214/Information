# -*- coding: utf-8 -*-
import os
import random
import re
import sys
import time
import platform
from urllib.parse import quote

import requests
from information.config import *
from gtaasmysql import MySQL
from loggtaas import Log
from lxml import etree

from gtaasutils import getHeaders


class Zhuankebaba(object):
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
        self.obj_mysql.create(SQL_CREATE_ZHUANKEBABA_TABLE)
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    # 抓取一级页面
    def get_page(self, keyword):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        url = 'https://www.xcpu.com.cn/?s=' + quote(keyword)
        try:
            response = requests.get(url=url, headers=getHeaders(), verify=False)
            if response.status_code == 200:
                self.logger.debug('Crawl succeeded: {}'.format(url))
                return response.text
            self.logger.debug('Crawl failed: {}'.format(url))
            return None
        except:
            self.logger.debug('Crawl failed: {}'.format(url))
            return None
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    # 提取二级页面URL
    def parse_page(self, response):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        if response:
            html = etree.HTML(response)
            items = html.xpath(
                '//section[@class="container"]/div[@class="content-wrap"]/div[@class="content"]/article[contains(@class, "excerpt")]')
            for item in items:
                url = item.xpath('./a[@class="focus"]/@href')[0]
                yield url
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    # 获取二级页面内容
    def get_sub_page(self, url):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        try:
            response = requests.get(url=url, headers=getHeaders())
            if response.status_code == 200:
                return response.text
            return None
        except:
            return None
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    # 结构化提取
    def parse_detail(self, response, url, keyword):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        if response:
            html = etree.HTML(response)
            result = {}
            body_classes = ''.join(html.xpath('//body/@class'))
            pattern = re.compile('.*postid-(\d+)\s.*')
            result['id'] = re.search(pattern, body_classes).group(1)
            result['keyword'] = keyword
            result['source'] = 'zhuankebaba'
            result['title'] = html.xpath(
                '//section[@class="container"]/div[@class="content-wrap"]/div[@class="content"]/header[@class="article-header"]/h1[@class="article-title"]/a/text()')[
                0]
            result['post_on'] = ''.join(html.xpath(
                '//section[@class="container"]/div[@class="content-wrap"]/div[@class="content"]/header[@class="article-header"]/div[@class="article-meta"]/span[1]/text()')).strip()
            result['category'] = ''.join(html.xpath(
                '//section[@class="container"]/div[@class="content-wrap"]/div[@class="content"]/header[@class="article-header"]/div[@class="article-meta"]/span[2]/a/text()')).strip()
            result['contact'] = ''.join(html.xpath(
                '//section[@class="container"]/div[@class="content-wrap"]/div[@class="content"]/div[@class="asb-post-footer"]/a/text()')).strip()
            result['tag'] = ''.join(html.xpath(
                '//section[@class="container"]/div[@class="content-wrap"]/div[@class="content"]/div[@class="article-tags"]/a/text()')).strip()
            items = html.xpath(
                '//section[@class="container"]/div[@class="content-wrap"]/div[@class="content"]/article[@class="article-content"]/div[contains(@class, "asb")]/following-sibling::*')
            result['content'] = ''
            for item in items:
                comment = (''.join(item.xpath('./text()'))).strip()
                result['content'] += comment
            result['url'] = url
            self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
            return result

    def run(self):
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        for keyword in KEYWORDS:
            for url in self.parse_page(self.get_page(keyword)):
                time.sleep(random.random() * 2)
                response = self.get_sub_page(url)
                result = self.parse_detail(self.get_sub_page(url), url, keyword)
                self.obj_mysql.insert(result, MYSQL_TABLE_ZHUANKEBABA)
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

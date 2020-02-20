# -*- coding: utf-8 -*-
import os
import sys
import time
import random
import platform
from urllib.parse import urljoin

import requests
from information.config import *
from loggtaas import Log
from lxml import etree
from datetime import datetime

from gtaasutils import format_datetime, getHeaders

requests.packages.urllib3.disable_warnings()


class InformationMetaclass(type):
    def __new__(cls, name, bases, attrs):
        count = 0
        attrs['__CrawlFunc__'] = []
        for k, v in attrs.items():
            if 'crawl_' in k:
                attrs['__CrawlFunc__'].append(k)
                count += 1
        attrs['__CrawlFuncCount__'] = count
        return type.__new__(cls, name, bases, attrs)


class Crawler(object, metaclass=InformationMetaclass):
    def __init__(self):
        """
            Initialize mysql, log
            Create mysql tables if not exists
        """
        name = os.path.split(__file__)[-1].split(".")[0]
        log_path = os.path.join(os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep), 'log')
        if platform.platform().startswith('Windows'):
            self.log = Log(name=name, path=log_path + '\\' + name + r".log", level='ERROR')
        else:
            self.log = Log(name=name, path=log_path + '/' + name + r".log", level='ERROR')
        self.logger = self.log.Logger

    def get_datas(self, callback):
        for item in eval("self.{}()".format(callback)):
            yield item

    def crawl_tao_rtinfo(self):
        """
        获取79淘实时线报
        :return: 元数据
        """
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        try:
            tao_url = 'http://www.79tao.com/portal.php'
            self.logger.debug('Crawling: %s' % tao_url)
            resp = requests.get(tao_url, headers=getHeaders(), verify=False)
            if resp.status_code == 200:
                html = etree.HTML(resp.content)
                items = html.xpath("//div[@class='dxb_bc']/div/ul/li/a[last()]")
                for item in items:
                    try:
                        data = {}
                        data['url'] = item.xpath('./@href')[0]
                        data['id'] = data['url'].split('-')[-3]
                        data['title'] = ''.join(item.xpath('./text()'))
                        result = {}
                        result['table'] = MYSQL_TABLE_TAO_RTINFO
                        result['data'] = data
                        yield result
                    except Exception as e:
                        self.logger.error('Failed to parse url: {}'.format(tao_url))
                        continue
        except Exception as e:
            self.logger.error('Failed to crawl: %s' % tao_url)
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    #
    # def crawl_tao_clipcoupons(self):
    #     """
    #     获取79淘薅羊毛
    #     :return: 元数据
    #     """
    #     self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
    #     try:
    #         tao_url = 'http://www.79tao.com/forum-46-1.html'
    #         self.logger.debug('Crawling: %s' % tao_url)
    #         resp = requests.get(tao_url, headers=getHeaders(), verify=False)
    #         if resp.status_code == 200:
    #             html = etree.HTML(resp.content)
    #             items = html.xpath(
    #                 "//table[@id='threadlisttableid']/tbody[contains(@id, 'normalthread')]")
    #             for item in items:
    #                 try:
    #                     data = {}
    #                     data['id'] = item.xpath('./@id')[0].split('_')[-1]
    #                     data['title'] = ''.join(item.xpath('./tr/th/a[2]/text()'))
    #                     data['url'] = item.xpath('./tr/th/a[2]/@href')[0]
    #                     result = {}
    #                     result['table'] = MYSQL_TABLE_TAO_CLIPCOUPONS
    #                     result['data'] = data
    #                     print(data)
    #                     yield result
    #                 except Exception as e:
    #                     self.logger.error('Failed to parse url: {}'.format(tao_url))
    #                     continue
    #     except Exception as e:
    #         self.logger.error('Failed to save data')
    #     self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
    #
    def crawl_tao_activityinfo(self):
        """
        获取79淘活动线报
        :return: 元数据
        """
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        try:
            tao_url = 'http://www.79tao.com/forum-44-1.html'
            self.logger.debug('Crawling: %s' % tao_url)
            resp = requests.get(tao_url, headers=getHeaders(), verify=False)
            if resp.status_code == 200:
                html = etree.HTML(resp.content)
                items = html.xpath(
                    "//table[@id='threadlisttableid']/tbody[contains(@id, 'normalthread')]")
                for item in items:
                    try:
                        data = {}
                        data['id'] = item.xpath('./@id')[0].split('_')[-1]
                        data['title'] = ''.join(item.xpath('./tr/th/a[2]/text()'))
                        data['url'] = item.xpath('./tr/th/a[2]/@href')[0]
                        data['dt'] = ''.join(item.xpath("./tr/td[@class='by']/em/span/text()"))
                        result = {}
                        result['table'] = MYSQL_TABLE_TAO_ACTIVITYINFO
                        result['data'] = data
                        print(data)
                        yield result
                    except Exception as e:
                        self.logger.error('Failed to parse url: {}'.format(tao_url))
                        continue
        except Exception as e:
            self.logger.error('Failed to crawl: %s' % tao_url)
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    # def crawl_zy_baicai(self):
    #     """
    #     获取zhuanyes白菜价
    #     :return: 元数据
    #     """
    #     self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
    #     zy_baicai_urls = ['https://www.zhuanyes.com/baicai/?mall=1', 'https://www.zhuanyes.com/baicai/?mall=2',
    #                       'https://www.zhuanyes.com/baicai/?mall=3', 'https://www.zhuanyes.com/baicai/?mall=4']
    #     try:
    #         for zy_url in zy_baicai_urls:
    #             self.logger.debug('Crawling: %s' %  zy_url)
    #             resp = requests.get(zy_url, headers=getHeaders(), verify=False)
    #             self.logger.debug('Crawling: {}'.format(zy_url))
    #             if resp.status_code == 200:
    #                 html = etree.HTML(resp.content)
    #                 items = html.xpath("//div[@id='item_list']/div")
    #                 for item in items:
    #                     try:
    #                         data = {}
    #                         data['id'] = (item.xpath('./@id')[0]).split('_')[-1]
    #                         data['url'] = item.xpath('./a/@href')[0]
    #                         data['title'] = ''.join(item.xpath(
    #                             './div[contains(@class, "item_info")]/div[contains(@class, "item_title")]//a/text()')) + ''.join(
    #                             item.xpath(
    #                                 './div[contains(@class, "item_info")]/div[contains(@class, "item_title")]//span/text()')
    #                         )
    #                         data['post_on'] = ''.join(item.xpath(
    #                             './div[contains(@class, "item_info")]/div[@class="item_foot"]//div[contains(@class, "date")]/span/@title'))
    #                         data['source'] = 'zhuanyes'
    #                         kw = zy_url.split('=')[-1]
    #                         if kw == '1':
    #                             data['keyword'] = '淘宝天猫'
    #                         elif kw == '2':
    #                             data['keyword'] = '京东'
    #                         elif kw == '3':
    #                             data['keyword'] = '苏宁'
    #                         elif kw == '4':
    #                             data['keyword'] = '当当'
    #                         else:
    #                             data['keyword'] = '未知'
    #                         response = requests.get(data['url'], headers=getHeaders(), verify=False)
    #                         if response.status_code == 200:
    #                             sub_html = etree.HTML(response.text)
    #                             data['content'] = ''.join(
    #                                 sub_html.xpath("//div[@class='detail_dinfo']/text()")).rstrip()
    #                         result = {}
    #                         result['table'] = MYSQL_TABLE_ZY_BAICAI
    #                         result['data'] = data
    #                         yield result
    #                     except Exception as e:
    #                         self.logger.error('Failed to parse url: {}'.format(zy_url))
    #                         continue
    #     except Exception as e:
    #         self.logger.error('Exception')
    #     self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
    #
    # def crawl_zy_tenyuanstore(self):
    #     """
    #     获取zhuanyes10元店
    #     :return: 元数据
    #     """
    #     self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
    #     zy_tenyuanstore_url = 'https://www.zhuanyes.com/ten/?page=1'
    #     try:
    #         self.logger.debug('Crawling: %s' %  zy_tenyuanstore_url)
    #         resp = requests.get(zy_tenyuanstore_url, headers=getHeaders(), verify=False)
    #         if resp.status_code == 200:
    #             html = etree.HTML(resp.content)
    #             items = html.xpath("//div[@id='couponlist']//div[contains(@class, 'detail_box')]")
    #             for item in items:
    #                 try:
    #                     data = {}
    #                     data['url'] = item.xpath('./a/@href')[0]
    #                     data['id'] = data['url'].split(r'%')[0].split('=')[1]
    #                     data['img'] = '[QQ:pic={}]'.format(item.xpath("./a/div[@class='de_img']/img/@src")[0])
    #                     data['title'] = ''.join(item.xpath("./a/@title"))
    #                     data['price'] = ''.join(
    #                         item.xpath("./div[@class='de_info']/em/text()")).strip() + " (" + ''.join(
    #                         item.xpath(".//span[@class='info_coupon']/text()")).strip() + ")"
    #                     result = {}
    #                     result['table'] = MYSQL_TABLE_ZY_TENYUANSTORE
    #                     result['data'] = data
    #                     yield result
    #                 except Exception as e:
    #                     self.logger.error('Failed to parse url: {}'.format(zy_tenyuanstore_url))
    #                     continue
    #     except Exception as e:
    #         self.logger.error('Exception')
    #     self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))
    #
    # def crawl_zhuanker_forum(self):
    #     """
    #     获取zhuanker论坛
    #     :return: 元数据
    #     """
    #     self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
    #     ak_forum_url = 'http://bbs.zhuanker.com/forum-37-1.html'
    #     try:
    #         self.logger.debug('Crawling: %s' % ak_forum_url)
    #         resp = requests.get(zy_tenyuanstore_url, headers=getHeaders(), verify=False)
    #         if resp.status_code == 200:
    #             html = etree.HTML(resp.content)
    #             items = html.xpath("//div[@id='couponlist']//div[contains(@class, 'detail_box')]")
    #             for item in items:
    #                 try:
    #                     data = {}
    #                     data['url'] = item.xpath('./a/@href')[0]
    #                     data['id'] = data['url'].split(r'%')[0].split('=')[1]
    #                     data['img'] = '[QQ:pic={}]'.format(item.xpath("./a/div[@class='de_img']/img/@src")[0])
    #                     data['title'] = ''.join(item.xpath("./a/@title"))
    #                     data['price'] = ''.join(
    #                         item.xpath("./div[@class='de_info']/em/text()")).strip() + " (" + ''.join(
    #                         item.xpath(".//span[@class='info_coupon']/text()")).strip() + ")"
    #                     result = {}
    #                     result['table'] = MYSQL_TABLE_ZY_TENYUANSTORE
    #                     result['data'] = data
    #                     yield result
    #                 except Exception as e:
    #                     self.logger.error('Failed to parse url: {}'.format(zy_tenyuanstore_url))
    #                     continue
    #     except Exception as e:
    #         self.logger.error('Exception')
    #     self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    def crawl_just_xianbao(self):
        """
        获取just999线报
        :return: 元数据
        """
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        just_xianbao_urls = ['https://just998.com/xianbao/suning', 'https://just998.com/xianbao/jd',
                             'https://just998.com/xianbao/tmall', 'https://just998.com/xianbao/miling']
        just_xianbao_part_url = 'https://just998.com'
        try:
            for just_url in just_xianbao_urls:
                self.logger.debug('Crawing: %s' % just_url)
                resp = requests.get(just_url, headers=getHeaders(), verify=False)
                if resp.status_code == 200:
                    html = etree.HTML(resp.content)
                    items = html.xpath(
                        "//div[@class='row']/div[1]/div[contains(@class, 'visited')]/div[contains(@class, 'panel-body')]/a")
                    for item in items:
                        try:
                            data = {}
                            data['url'] = urljoin(just_xianbao_part_url, item.xpath('./@href')[0])
                            data['id'] = (data['url'].split('/')[-1]).split('.')[0]
                            data['title'] = ''.join(item.xpath('./@title'))
                            data['post_on'] = ''.join(item.xpath('.//small[@class="mr-5"]/text()')).strip()
                            data['source'] = 'just999'
                            kw = just_url.split('/')[-1]
                            if kw == 'suning':
                                data['keyword'] = '苏宁线报'
                            elif kw == 'jd':
                                data['keyword'] = '京东线报'
                            elif kw == 'tmall':
                                data['keyword'] = '天猫线报'
                            elif kw == 'miling':
                                data['keyword'] = '羊毛线报'
                            result = {}
                            result['table'] = MYSQL_TABLE_JUST_XIANBAO
                            result['data'] = data
                            yield result
                        except Exception as e:
                            self.logger.error('Failed to parse url: {}'.format(just_url))
                            continue
        except Exception as e:
            self.logger.error('Failed to save data')
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

    def crawl_tuan_xianbao(self):
        """
        获取tuan0818线报
        :return: 元数据
        """
        self.logger.debug('Enter func:【{}】'.format(sys._getframe().f_code.co_name))
        for index in range(33, 37):
            time.sleep(random.random() * 2)
            url = 'http://www.0818tuan.com/e/search/result/?searchid={}'.format(str(index))
            part_url = 'http://www.0818tuan.com/'
            try:
                self.logger.debug("Crawling: {}".format(url))
                resp = requests.get(url, headers=getHeaders(), verify=False)
                if resp.status_code == 200:
                    html = etree.HTML(resp.content)
                    items = html.xpath('//div[@class="list-group"]/a')
                    for item in items:
                        data = {}
                        data['url'] = urljoin(part_url, item.xpath('./@href')[0])
                        data['id'] = (data['url'].split('/')[-1]).split('.')[0]
                        data['title'] = ''.join(item.xpath('./@title'))
                        year = datetime.strftime(datetime.now(), '%Y')
                        data['post_on'] = year + '-' + ''.join(item.xpath('./span/text()')).strip()
                        data['source'] = '0818tuan'
                        data['keyword'] = html.xpath("//input[@id='keyboard']/@value")[0]
                        result = {}
                        result['table'] = MYSQL_TABLE_TUAN_XIANBAO
                        result['data'] = data
                        yield result
            except Exception as e:
                self.logger.error('Failed to parse')
        self.logger.debug('Leave func:【{}】'.format(sys._getframe().f_code.co_name))

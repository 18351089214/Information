# -*- coding: utf-8 -*-
from information.crawler import Crawler
from information.config import *
from gtaasmysql import MySQL


class Getter():
    def __init__(self):
        self.crawler = Crawler()
        self.obj_mysql = MySQL(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_DATABASE)
        self.obj_mysql.create(SQL_CREATE_ZY_TENYUANSTORE_TABLE)
        self.obj_mysql.create(SQL_CREATE_ZY_BAICAI_TABLE)
        self.obj_mysql.create(SQL_CREATE_JUST_TABLE)
        self.obj_mysql.create(SQL_CREATE_TUANXIANBAO_TABLE)
        self.obj_mysql.create(SQL_CREATE_TAO_ACTIVITYINFO_TABLE)
        self.obj_mysql.create(SQL_CREATE_TAO_RTINFO_TABLE)
        self.obj_mysql.create(SQL_CREATE_TAO_CLIPCOUPONS_TABLE)

    def run(self):
        for callback_label in range(self.crawler.__CrawlFuncCount__):
            callback = self.crawler.__CrawlFunc__[callback_label]
            print(callback + ' to run')
            for item in self.crawler.get_datas(callback):
                self.obj_mysql.insert(item['data'], item['table'])

    def __del__(self):
        self.obj_mysql.close()

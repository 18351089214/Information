# -*- coding: utf-8 -*-
import time
from information.getter import Getter
from information.pojie import Pojie
from information.zhuankebaba import Zhuankebaba
from information.weibo import Weibo
from information.config import *
from multiprocessing import Process


class Scheduler(object):
    def schedule_getter(self, cycle=GETTER_CYCLE):
        """
        定时采集数据
        :param cycle: 周期
        :return:
        """
        getter = Getter()
        while True:
            print('开始采集数据')
            getter.run()
            time.sleep(cycle)

    def schedule_pojie(self, cycle=POJIE_CYCLE):
        """
        定时采集52pojie
        :param cycle: 周期
        :return:
        """
        pojie = Pojie()
        while True:
            print('开始采集52pojie数据')
            pojie.run()
            time.sleep(cycle)

    def schedule_zhuankebaba(self, cycle=ZHUANKEBABA_CYCLE):
        """
        周期采集zhuankebaba
        :param cycle: 周期
        :return:
        """
        zkbb = Zhuankebaba()
        while True:
            print('开始采集zhuankebaba')
            zkbb.run()
            time.sleep(cycle)

    def schedule_weibo(self, cycle=WEIBO_CYCLE):
        """
        周期采集微博
        :param cycle: 微博
        :return:
        """
        weibo = Weibo()
        while True:
            print('开始采集微博')
            weibo.run()
            time.sleep(cycle)

    def run(self):
        if ENABLE_GETTER:
            getter_process = Process(target=self.schedule_getter)
            getter_process.start()

        if ENABLE_POJIE:
            pojie_process = Process(target=self.schedule_pojie)
            pojie_process.start()

        if ENABLE_ZHUANKEBABA:
            zkbb_process = Process(target=self.schedule_zhuankebaba)
            zkbb_process.start()

        if ENABLE_WEIBO:
            weibo_process = Process(target=self.schedule_weibo)
            weibo_process.start()

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time
import os
from proj_common.conf_util import ConfigUtils
from logging.handlers import TimedRotatingFileHandler
from logging import Filter
"""
日志模块：
    1、按天分文件保存日志
    2、支持保存时长（默认历史7份）
    3、自定义日志目录后缀：set_log_path_suffix
        用作统一项目下多个程序或进程日志输出不同目录
        eg(引入包后立即设置):
            from common.logger import Logger
            Logger.set_log_path_suffix("index_and_trace")
"""


class LevelFilter(Filter):
    
    def __init__(self, name='', level=logging.INFO):
        Filter.__init__(self, name)
        self.level = level

    def filter(self, record):
        return record.levelno == self.level


class Logger:

    pro_log_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    # 记录日志 sys,debug,info,warning,critical,error
    dateformat = '%Y-%m-%d %H:%M:%S'
    logger = logging.getLogger('pyspark')
    logger.setLevel(logging.DEBUG)

    # 注意文件内容写入时编码格式指定

    log_path = ConfigUtils.get_conf("sys", "log.path") or os.path.join(pro_log_path, "logs")
    log_levels = [x.upper() for x in (ConfigUtils.get_conf("sys", "log.levels") or "sys").split(",")]
    backup_count = ConfigUtils.get_conf("sys", "log.maxHistory") or 7
    if not os.path.isabs(log_path):
        log_path = os.path.join(pro_log_path, log_path)

    @classmethod
    def init_handlers(cls):

        if not os.path.exists(cls.log_path):
            os.makedirs(cls.log_path)

        for l in cls.log_levels:
            if l in ["SYS", "DEBUG", "INFO", "WARNING", "CRITICAL", "ERROR"]:
                l_path = os.path.join(cls.log_path, "{}.log".format(l.lower()))
                if not os.path.exists(l_path):
                    f = open(l_path, 'w')
                    f.close()

                fh = TimedRotatingFileHandler(l_path, when="D", encoding='utf-8', backupCount=cls.backup_count)

                if l == "SYS":
                    fh.setLevel(logging.DEBUG)
                    cls.logger.addHandler(fh)
                else:
                    fh.setLevel(logging.getLevelName(l))
                    fh.addFilter(LevelFilter(level=logging.getLevelName(l)))
                    cls.logger.addHandler(fh)

    @classmethod
    def set_log_path_suffix(cls, suffix):
        cls.log_path = "{}_{}".format(cls.log_path, suffix)
        cls.init_handlers()

    @staticmethod
    def debug(log_message):
        Logger.logger.debug("[DEBUG "+get_current_time()+"] "+log_message)

    @staticmethod
    def info(log_message):
        Logger.info_prefix("[INFO "+get_current_time()+"]", log_message)

    @staticmethod
    def info_prefix(prefix, log_message):
        Logger.logger.info(prefix+" "+log_message)

    @staticmethod
    def warning(log_message):
        Logger.logger.warning("[WARNING " + get_current_time() + "] " + log_message)

    @staticmethod
    def error(log_message):
        Logger.logger.error("[ERROR " + get_current_time() + "] " + log_message)

    @staticmethod
    def exception(log_message):
        Logger.logger.exception("[ERROR " + get_current_time() + "] " + log_message)

    @staticmethod
    def critical(log_message):
        Logger.logger.critical("[CRITICAL " + get_current_time() + "] " + log_message)

# logger可以看做是一个记录日志的人，对于记录的每个日志，他需要有一套规则，比如记录的格式（formatter），
# 等级（level）等等，这个规则就是handler。使用logger.addHandler(handler)添加多个规则，
# 就可以让一个logger记录多个日志。


def get_current_time():
    return time.strftime(Logger.dateformat, time.localtime(time.time()))


Logger.init_handlers()

# Logger.info("ddd")

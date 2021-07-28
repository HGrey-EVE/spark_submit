#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configparser import ConfigParser
import os


class ConfigUtils(object):
    
    __instance = None

    def __init__(self):
        super(ConfigUtils, self).__init__()
        try:
            self.path = os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                    "conf"
                )
            )
            self.config = ConfigParser()
            active = "dev"
            cfg_files = [
                os.path.join(self.path, "application."+active+".conf")
            ]
            print(cfg_files)
            self.config.read(cfg_files, encoding="utf-8")
        except IOError as e:
            print(e)

    @classmethod
    def _instance(cls):
        if cls.__instance is None:
            cls.__instance = ConfigUtils()

        return cls.__instance

    @classmethod
    def get_conf(cls, section, option):
        """
        获取配置项
        :param section:
        :param option:
        :return:
        """
        cls._instance()
        value = cls.__instance.config.get(section, option)
        if value is None or value.strip() == '':
            return None
        return value

    @classmethod
    def get_sec_conf(cls, section):
        """
        字典形式返回一个 大项 下的所有配置
        :param section:
        :return:
        """
        cls._instance()
        values = cls.__instance.config.items(section)
        if values:
            return {k: v for k, v in values}
        else:
            return None

        return values


if __name__ == '__main__':
    pass
    # print(ConfigUtils.get_sec_conf("spark-submit-conf"))


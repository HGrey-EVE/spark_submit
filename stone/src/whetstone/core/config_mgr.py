#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-12-20 16:51
"""
import configparser
import os
import typing

from whetstone.common.settings import Setting
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo


class ConfigItem:
    def __init__(self, *, section, key, value):
        self._section = section
        self._key = key
        self._value = value

    def __repr__(self):
        return f"{self._section} {self._key} {self._value}"

    @property
    def section(self):
        return self._section

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._value


def parser_2_items(parser: configparser.ConfigParser) -> typing.List[ConfigItem]:
    items = []
    for section in parser.sections():
        for key, value in parser[section].items():
            items.append(ConfigItem(section=section,
                                    key=key,
                                    value=value))
    return items


class Config:
    def __init__(self, path: str = None):
        self._config = configparser.ConfigParser()
        if not path:
            self._items = []
            self._config_path = ""
        else:
            self._config_path = path
            self._config.read(self._config_path)
            self._items = parser_2_items(self._config)

    def __str__(self):
        return f"""
        path: {self._config_path}
        items: {self._items}
        """

    def options(self, section):
        return self._config.options(section)

    def sections(self):
        return self._config.sections()

    def set(self, section, option, value=None):
        if section not in self._config.sections():
            self._config.add_section(section)
        self._config.set(section=section, option=option, value=value)
        self._items = parser_2_items(self._config)

    def get(self, section, option):
        return self._config.get(section, option, fallback=None)

    def clone(self):
        return Config(self._config_path)

    @property
    def config_path(self):
        return self._config_path

    def merge(self, other):
        """
        merge config from other config, overwrite value if it have save config item key
        :param other: Config instance
        :return: a new deep copy config instance
        """
        config = self.clone()
        if isinstance(other, (Config,)):
            for item in other.items:
                config.set(item.section, item.key, item.value)
        return config

    @property
    def items(self) -> typing.List[ConfigItem]:
        return self._items

    def __iter__(self):
        return iter(self._items)

    def write(self):
        with open(self._config_path) as fp:
            self._config.write(fp)


class ConfigManager:

    def __init__(self, proc: ProcInfo, settings: Setting):
        self._proc = proc
        project = Config(PathManager.get_project_conf_path(proc, settings))
        module = Config(PathManager.get_module_conf_path(proc, settings))
        self._configs = {"project": project, "module": module, "merged": project.merge(module)}
        self._hdfs = ConfigManager.HDFS(proc, settings, self)

    class HDFS:

        def __init__(self, proc: ProcInfo, settings: Setting, cfg_mgr):
            self._proc = proc
            self._settings = settings
            self._cfg_mgr = cfg_mgr

        def _get_path(self, section, option, _type, default=None):
            value = self._cfg_mgr.get(section, option) or default
            if not value:
                return
            root_path = self._cfg_mgr.get("hdfs", "hdfs_root_path")
            return os.path.join(root_path, self._proc.version, _type, value)

        def get_fixed_path(self, section, option):
            value = self._cfg_mgr.get(section, option)
            root_path = self._cfg_mgr.get("hdfs", "hdfs_root_path")
            return os.path.join(root_path, value)

        def get_result_path(self, section, option, default=None):
            return self._get_path(section, option, "result", default)

        def get_tmp_path(self, section, option, default=None):
            return self._get_path(section, option, "tmp", default)

        def get_input_path(self, section, option, default=None):
            return self._get_path(section, option, "input", default)

    @property
    def hdfs(self):
        return self._hdfs

    @property
    def config(self) -> Config:
        return self._configs["merged"]

    @property
    def project(self) -> Config:
        return self._configs["project"]

    def get(self, section, option):
        return self.config.get(section, option)

    @property
    def module(self) -> Config:
        return self._configs["module"]

    def write(self):
        for cfg in self._configs.values():
            cfg.write()
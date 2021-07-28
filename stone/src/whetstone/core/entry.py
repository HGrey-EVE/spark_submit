#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-03-16 16:21
"""
from logging import Logger

from pyspark.sql import SparkSession

from whetstone.common.settings import Setting
from whetstone.core.config_mgr import ConfigManager
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo


class Entry:

    def __init__(self):
        self._spark_session = None
        self._logger = None
        self._cfg_mgr = None
        self._path_mgr = None
        self._proc = None
        self._settings = None

    @property
    def fresh(self):
        return True

    @property
    def version(self):
        return self.proc.version

    @property
    def settings(self):
        return self._settings

    @property
    def path_mgr(self) -> PathManager:
        return self._path_mgr

    @property
    def spark(self) -> SparkSession:
        return self._spark_session

    @property
    def proc(self) -> ProcInfo:
        return self._proc

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def cfg_mgr(self) -> ConfigManager:
        return self._cfg_mgr

    def build_spark(self, session: SparkSession):
        self._spark_session = session
        return self

    def build_logger(self, logger: Logger):
        self._logger = logger
        return self

    def build_cfg_mgr(self, cfg_mgr: ConfigManager):
        self._cfg_mgr = cfg_mgr
        return self

    def build_path_mgr(self, path_mgr: PathManager):
        self._path_mgr = path_mgr
        return self

    def build_proc(self, proc: ProcInfo):
        self._proc = proc
        return self

    def build_settings(self, settings: Setting):
        self._settings = settings
        return self

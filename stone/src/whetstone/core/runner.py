#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-12-20 16:51
"""
import importlib
import os
import sys
import traceback

# insert whetstone python path, and this should locate before any other code
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from pyspark import SparkConf
from pyspark.sql import SparkSession
from whetstone.core.entry import Entry
from whetstone.core.config_mgr import ConfigManager
from whetstone.core.path_mgr import PathManager
from whetstone.common.exception import FriendlyException
from whetstone.common.logging1 import get_logger
from whetstone.core.proc import ProcInfo, get_python_cmd_args
from whetstone.common.settings import Setting


def get_runner_path():
    return os.path.realpath(__file__)


def check_index_module_valid(target, settings: Setting):
    func_names = settings.get_default_index_module_func()
    valid = True
    for func_name in func_names:
        func = getattr(target, func_name)
        valid = valid and bool(callable(func))
    return valid


def spark_cluster_mode(spark):
    return True if spark.conf.get("spark.submit.deployMode") == "cluster" else False


def patch_spark_cluster_source_root_path(proc: ProcInfo, settings: Setting):
    settings.get_spark_source_root_path = lambda: os.path.join(os.getcwd(), f"project-{proc.project}/")

def run(proc: ProcInfo, cfg_mgr: ConfigManager, settings: Setting, logger):
    spark = get_spark_session(proc, cfg_mgr, settings, logger)
    if spark_cluster_mode(spark):
        patch_spark_cluster_source_root_path(proc, settings)
        cfg_mgr = ConfigManager(_proc, _settings)
    # insert spark project root path as python path
    sys.path.insert(0, settings.get_spark_source_root_path())
    target = importlib.import_module(PathManager.get_module_py_path(proc))
    if not check_index_module_valid(target, settings):
        raise FriendlyException(f"project {proc.project} module {proc.module} "
                                f"do not contains valid entry ")

    entry = Entry() \
        .build_spark(spark) \
        .build_cfg_mgr(cfg_mgr) \
        .build_logger(logger) \
        .build_proc(_proc)
    if bool(target.pre_check(entry)):
        try:
            target.main(entry)
        except:
            raise
        finally:
            target.post_check(entry)
    else:
        raise FriendlyException(f"project {proc.project} module {proc.module} "
                                f"pre check do not passed")


def get_spark_conf(cfg_mgr: ConfigManager, settings: Setting, logger) -> SparkConf:
    conf = SparkConf()
    for item in cfg_mgr.config.items:
        if item.section in [settings.get_default_conf_spark_conf_section_name()]:
            logger.info(f"spark conf {item.key}: {item.value}")
            conf.set(item.key, item.value)
    return conf


def get_spark_session(proc: ProcInfo, cfg_mgr: ConfigManager, settings: Setting, logger) -> SparkSession:
    spark = SparkSession \
        .builder \
        .appName(f"{proc.project}-{proc.module}-{proc.env}") \
        .config(conf=get_spark_conf(cfg_mgr, settings, logger)) \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


if __name__ == '__main__':
    _proc = get_python_cmd_args()
    _settings = Setting()
    _cfg_mgr = ConfigManager(_proc, _settings)

    _logger = get_logger(_proc, _settings)
    try:
        run(_proc, _cfg_mgr, _settings, _logger)
    except FriendlyException as err:
        _logger.error(err)
        _logger.error(traceback.format_exc())
        exit(-1)
    except Exception as err:
        _logger.error(err)
        _logger.error(traceback.format_exc())
        exit(-1)

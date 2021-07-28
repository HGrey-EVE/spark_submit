#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-12-20 16:51
"""
import datetime
import logging
import os
import sys

from whetstone.common.settings import Setting
from whetstone.core.proc import ProcInfo


def get_logger(proc: ProcInfo, settings):
    log_folder_path = _get_log_folder_path(proc, settings)
    log_file_name = _get_log_file_name(proc)
    fmt = '%(asctime)s - %(filename)s[%(lineno)d] - %(levelname)s: %(message)s'
    formatter = logging.Formatter(fmt)
    logger = logging.getLogger(f"{proc.project}-{proc.module}")

    file_handler = logging.FileHandler(os.path.join(*[log_folder_path, log_file_name]))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    return logger


def _get_log_file_name(proc: ProcInfo) -> str:
    ret = f"{proc.project}-{proc.module}-{datetime.datetime.now().strftime('%Y-%m-%d@%H%M%S')}.log"
    return ret


def _get_log_folder_path(proc: ProcInfo, settings: Setting) -> str:
    log_root_path = settings.get_application_log_root_path()
    if not log_root_path:
        log_root_path = settings.get_spark_source_root_path() or "."
        log_root_path = os.path.join(log_root_path, "logs")
    path_nodes = [log_root_path, proc.project, proc.module]
    log_folder_path = os.path.join(*path_nodes)
    os.path.exists(log_folder_path) or os.makedirs(log_folder_path, exist_ok=True)
    return log_folder_path

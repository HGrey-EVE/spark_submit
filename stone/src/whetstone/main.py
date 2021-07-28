#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-12-20 16:51
"""

import os
import subprocess
import sys


def get_framework_source_folder():
    return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


# insert whetstone python path, and this should locate before any other code
sys.path.insert(0, get_framework_source_folder())

from whetstone.core.config_mgr import ConfigManager
from whetstone.common.logging1 import get_logger
from whetstone.core.proc import ProcInfo, get_python_cmd_args

from whetstone.common.settings import Setting
from whetstone.core.submiter import get_spark_submit_cmd
from whetstone.utils.utils_python_path import PythonPathUtil


def main(_proc: ProcInfo, cfg_mgr: ConfigManager, settings: Setting, logger):
    cmd = get_spark_submit_cmd(_proc, cfg_mgr, settings, get_runner_path())
    logger.info(f"cmd: {cmd}")
    sub = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True,
        universal_newlines=True
    )
    while sub.poll() is None:
        i = sub.stdout.readline()[:-1]
        if i and _proc.debug:
            logger.debug(i)
    if sub.returncode != 0:
        logger.info(f"{_proc.project}-{_proc.module} executed failed")
        lines = sub.stdout.readlines()
        for i in lines:
            logger.error(i)
        sub.terminate()
    else:
        sub.wait()
        logger.info(f"{_proc.project}-{_proc.module} executed finish")


if __name__ == '__main__':
    _proc = get_python_cmd_args()
    _settings = Setting()

    _cfg_mgr = ConfigManager(_proc, _settings)

    _logger = get_logger(_proc, _settings)
    _logger.info(f"spark_source_root_path: {_settings.get_spark_source_root_path()}", )

    PythonPathUtil.set_pyspark_path(_cfg_mgr.get('pyspark-env', 'spark_home'))
    from whetstone.core.runner import get_runner_path

    main(_proc, _cfg_mgr, _settings, _logger)

#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-14 18:13
"""
import logging
import subprocess
from logging import Logger


def run_cmd(cmd: str, logger: Logger = None):
    logger.debug(f"CMD:{cmd}")
    sub = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True
    )
    logger = logger or logging.getLogger()
    stdout, std_error = sub.communicate()
    logger.debug(f"CMD:{cmd}, return code: {sub.returncode}")
    logger.debug(stdout or std_error)
    return sub.returncode, stdout, std_error


class Shell:

    def __init__(self, logger: Logger = None):
        self._return_code = -1
        self._stdout = None
        self._std_error = None
        self._logger = logger or logging.getLogger()
        self._cmd_parts = []

    @property
    def cmd_parts(self):
        return self._cmd_parts

    @property
    def return_code(self):
        return self._return_code

    @property
    def stdout(self):
        return self._stdout

    @property
    def std_error(self):
        return self._std_error

    def build(self, *part):
        self._cmd_parts.extend([str(x).strip() for x in part if str(x).strip()])
        return self

    def __iter__(self):
        return (x for x in self._cmd_parts if x)

    def __str__(self):
        return " ".join(self)

    def execute(self):
        cmd = str(self)
        if cmd:
            self._return_code, self._stdout, self._std_error = run_cmd(cmd, self._logger)
            self._logger.debug(f"CMD:{cmd}, result stat: {self._return_code}")
        return self

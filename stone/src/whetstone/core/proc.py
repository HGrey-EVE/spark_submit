#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-02-03 16:15
"""
import argparse


class ProcInfo:
    def __init__(self, *, project, module, env, script=None, debug=False, version=""):
        self._project = project
        self._module = module
        self._env = env
        self._script = script
        self._debug = debug
        self._version = version

    @property
    def project(self):
        return self._project

    @property
    def debug(self):
        return self._debug

    @property
    def module(self):
        return self._module

    @property
    def env(self):
        return self._env

    @property
    def script(self):
        return self._script

    @property
    def version(self):
        return self._version


def get_python_cmd_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--project', type=str, required=True, help="spark project name", dest="project")
    parser.add_argument('-m', '--module', type=str, required=True, help="spark project module name", dest="module")
    parser.add_argument('-e', '--env', type=str, help="configuration env name", dest="env", default="")
    parser.add_argument('-d', '--debug', help="spark debug log", dest="debug", default=False, action="store_true")
    parser.add_argument('-t', '--script', type=str, help="spark script file name", dest="script", default="")
    parser.add_argument('-v', '--version', type=str, help="data version num", dest="version", default="")
    args = parser.parse_args()

    _proc = ProcInfo(**{
        "project": args.project,
        "module": args.module,
        "env": args.env,
        "script": args.script,
        "debug": args.debug,
        "version": args.version
    })
    return _proc
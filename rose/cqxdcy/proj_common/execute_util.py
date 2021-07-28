#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import subprocess
from proj_common.logger_util import Logger


class ExecutePyShell:
    """
        :param python_path: python路径
        :param project: 项目名称
        :param model: 模块名称
        :param script: 脚本名称
        :param env: 执行环境
        :param version: 版本控制
        :return:
    """
    @classmethod
    def execute_shell(cls, python_path, project, model, script, env, version):
        submit_cmd = f"""
            {python_path} /data2/cqxdcyfzyjy/lefeng/spark_plan_zero/whetstone/src/whetstone/main.py -p {project} -m {model} -t {script} -e {env} -v {version}
        """
        print("执行命令 - " + submit_cmd)

        flag = cls.exeute(submit_cmd)
        return flag

    """
        :param python_path: python路径
        :param model_basic_path: 模型基本路径
        :param script: 脚本名称
        :param version: 版本控制
        :return:
    """
    @classmethod
    def execute_shell_model(cls, python_path, model_basic_path, script, version):
        submit_cmd = f"""
            {python_path} {os.path.join(model_basic_path, script)} {version}
        """
        print("执行命令 - " + submit_cmd)

        flag = cls.exeute(submit_cmd)
        return flag

    @classmethod
    def execute_shell_model_hdfs(cls, model_basic_path, version):
        submit_cmd = f"""
            hdfs dfs -rm -r /user/cqxdcyfzyjy/{version}/tmp/cqxdcy/model; 
            hdfs dfs -mkdir /user/cqxdcyfzyjy/{version}/tmp/cqxdcy/model; 
            hdfs dfs -put {model_basic_path}/*result.csv /user/cqxdcyfzyjy/{version}/tmp/cqxdcy/model/
            """
        print("执行命令 - " + submit_cmd)

        flag = cls.exeute(submit_cmd)
        return flag

    @classmethod
    def exeute(cls, submit_cmd: str):
        sub = subprocess.Popen(
            submit_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            universal_newlines=True
        )

        while sub.poll() is None:
            i = sub.stdout.readline()[:-1]
            if i:
                print("[main]", i)

        if sub.returncode != 0:
            print("\n************* execute script -{}- error ~~ *****************".format(script))
            return 'fail'
if __name__ == '__main__':
    pass

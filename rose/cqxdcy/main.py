#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import datetime
import time

from proj_common.conf_util import ConfigUtils
from proj_common.execute_util import ExecutePyShell
from proj_common.date_util import DateUtils

def py_files():
    """
    打包工具包
    :param pwd_path:
    :return:
    """
    print("开始打包～")
    os.system("rm ./proj_common.zip; zip -r ./proj_common.zip ./proj_common __init__.py")


if __name__ == '__main__':
    # 打包
    py_files()

    python_path = ConfigUtils.get_conf("spark-submit-conf","spark.pyspark.python")
    print("[main] python版本路径 -- " + python_path)
    error_info_list = []

    while True:
        print("[main] 先睡3秒再说～")
        time.sleep(3)
        try:
            version = DateUtils.add_date_2str(date_time = datetime.datetime.now(), fmt = "%Y%m", months = -1) + "01"
            ## 获取传入进来的版本信息
            if len(sys.argv) > 1:
                print("[main] 获取传入进来的版本信息" + str(sys.argv[1]))
                version = sys.argv[1]
            print(f'[main] start execute version -> {version}')
            error_info_list = []

            model_basic_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "model/cqxdcy_model")


            print("[main] 「 产业划分 」执行 ---")
            flag = ExecutePyShell.execute_shell(
                python_path=python_path,
                project = "cqxdcy",
                model = "cqxdcy_req_divide",
                script = "cqxdcy_industry_divide",
                env = "dev",
                version = version
            )
            if flag == 'fail':
                error_info_list.append("""
                    产业划分计算失败---
                """)

            print("[main] 「 风险指标 」执行 ---")
            flag = ExecutePyShell.execute_shell(
                python_path=python_path,
                project="cqxdcy",
                model="cqxdcy_req_risk",
                script="risk_index",
                env="dev",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    ---产业分布计算失败---
                """)

            print("[main] 「 投资机构 」执行 ---")
            flag = ExecutePyShell.execute_shell(
                python_path=python_path,
                project="cqxdcy",
                model="cqxdcy_req_tzjg",
                script="cqxdcy_tzjg",
                env="dev",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    ---投资机构计算失败---
                """)

            print("[main] 「 投资机构 」执行 ---")
            flag = ExecutePyShell.execute_shell(
                python_path=python_path,
                project="cqxdcy",
                model="cqxdcy_req_tzjg",
                script="cqxdcy_tzjg",
                env="dev",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                                ---投资机构计算失败---
                """)

            print("[main] 「 招商价值 」执行 ---")
            flag = ExecutePyShell.execute_shell(
                python_path=python_path,
                project="cqxdcy",
                model="cqxdcy_req_zs",
                script="cqxdcy_zsjz",
                env="dev",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    招商价值计算失败---
                """)

            print("[main] 「 招商可行性 」执行 ---")
            flag = ExecutePyShell.execute_shell(
                python_path=python_path,
                project="cqxdcy",
                model="cqxdcy_req_zs",
                script="cqxdcy_zskxx",
                env="dev",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    招商可行性计算失败---
                """)

            print("[main] 「 招商模型、风险模型、投资模型脚本 」执行 ---")
            flag = ExecutePyShell.execute_shell_model(
                python_path=python_path,
                model_basic_path = model_basic_path,
                script="main.py",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    模型main脚本计算失败---
                """)

            print("[main] 「 招商模型、风险模型、投资模型输入数据put到hdfs 」执行 ---")
            flag = ExecutePyShell.execute_shell_model_hdfs(
                model_basic_path=model_basic_path,
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                   招商模型脚本计算失败---
                """)

            print("[main] 「 科技型 」执行 ---")
            flag = ExecutePyShell.execute_shell(
                python_path=python_path,
                project="cqxdcy",
                model="cqxdcy_req_kjx",
                script="cqxdcy_kjx_index",
                env="dev",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    科技型计算失败---
                """)

            print("[main] 「 kjx模型脚本 」执行 ---")
            flag = ExecutePyShell.execute_shell_model(
                python_path=python_path,
                model_basic_path=model_basic_path,
                script="main_model_kjx.py",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    kjx模型脚本计算失败---
                """)

            print("[main] 「 kjx模型put到hdfs上 」执行 ---")
            flag = ExecutePyShell.execute_shell_model_hdfs(
                model_basic_path=model_basic_path,
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    kjx模型put到hdfs上脚本计算失败---
                """)

            print("[main] 「 招商模型 」执行 ---")
            flag = ExecutePyShell.execute_shell(
                python_path=python_path,
                project="cqxdcy",
                model="cqxdcy_req_zs",
                script="cqxdcy_zs_model",
                env="dev",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    招商模型计算失败---
                """)

            print("[main] 「 风险模型 」执行 ---")
            flag = ExecutePyShell.execute_shell(
                python_path=python_path,
                project="cqxdcy",
                model="cqxdcy_req_risk",
                script="risk_model",
                env="dev",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    风险模型计算失败---
                """)

            print("[main] 「 科技型模型 」执行 ---")
            flag = ExecutePyShell.execute_shell(
                python_path=python_path,
                project="cqxdcy",
                model="cqxdcy_req_kjx",
                script="cqxdcy_kjx_model",
                env="dev",
                version=version
            )
            if flag == 'fail':
                error_info_list.append("""
                    科技型模型计算失败---
                """)

            if len(error_info_list) == 0:
                print("[main] 执行完成 -- 跳出循环～")
                print('[main] ok')
            else:
                print(error_info_list)
            break

        except Exception as e:
            print("[main] ---异常信息如下～～")
            print(e)
            break

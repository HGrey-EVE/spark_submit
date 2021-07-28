#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: utils_python_path
:Author: xufeng@bbdservice.com 
:Date: 2021-04-26 11:26 AM
:Version: v.1.0
:Description:
"""
import os
import sys

from whetstone.common.exception import FriendlyException


class PythonPathUtil(object):

    PYSPARK_ZIP_NAME = 'pyspark.zip'
    # PY4J_ZIP_NAME = 'py4j-0.10.7-src.zip'

    @staticmethod
    def _get_py4j_file_name(spark_lib_path):
        for f in os.listdir(spark_lib_path):
            if f.startswith('py4j-') and f.endswith('-src.zip'):
                return os.path.join(spark_lib_path, f)
        raise FriendlyException('pyspark py4j path not found')

    @staticmethod
    def _get_pyspark_py4j_path(spark_home: str) -> (str, str):
        lib_dir = os.path.join(spark_home, 'python', 'lib')
        pyspark_path = os.path.join(lib_dir, PythonPathUtil.PYSPARK_ZIP_NAME)
        py4j_path = PythonPathUtil._get_py4j_file_name(lib_dir)

        if os.path.exists(pyspark_path) and os.path.exists(py4j_path):
            return pyspark_path, py4j_path
        raise FriendlyException('pyspark path not found')

    @staticmethod
    def set_pyspark_path(spark_home: str = None):
        """
        get pyspark python and add it to current python path
        if python lib not has pyspark, you should set `spark_home` in the config file
        if python lib has pyspark, we recommend you not set spark home,
        :return:and here will return directly
        """
        if not spark_home:
            # raise FriendlyException('spark home is null')
            return

        if not os.path.exists(spark_home):
            raise FriendlyException(f'invalid spark home:{spark_home}')

        py_spark_path, py4j_path = PythonPathUtil._get_pyspark_py4j_path(spark_home)
        sys.path.insert(0, py_spark_path)
        sys.path.insert(0, py4j_path)


if __name__ == '__main__':
    PythonPathUtil.set_pyspark_path('/opt/spark-2.4.2')
    import pyspark

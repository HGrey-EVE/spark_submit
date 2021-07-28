#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-12-20 16:51
"""
import os
import sys

from whetstone.utils.utils_python_path import PythonPathUtil


def test_set_pyspark_path():
    spark_home = os.path.join(os.path.dirname(os.path.realpath(__file__)), "spark_home")
    PythonPathUtil.set_pyspark_path(spark_home)
    assert os.path.join(spark_home, "python", "lib", "pyspark.zip") in sys.path
    assert os.path.join(spark_home, "python", "lib", "py4j-0.10.X-src.zip") in sys.path

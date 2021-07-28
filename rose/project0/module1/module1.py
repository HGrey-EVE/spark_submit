#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-02-03 16:08
"""

from whetstone.core.entry import Entry


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    df = entry.spark.createDataFrame([["1", "指标1", 1], ["1", "指标2", 2], ["2", "指标1", 3], ["2", "指标2", 4]],
                                     ["id", "index", "value"])
    df.show()
    entry.logger.info(entry.cfg_mgr.get("spark-submit-opt", "queue"))
    entry.logger.info("wa, ha ha")
    entry.logger.info(entry.version)
    print("bang, ca ca")


def post_check(entry: Entry):
    return True

#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: basic_main
:Author: xufeng@bbdservice.com 
:Date: 2021-05-27 3:37 PM
:Version: v.1.0
:Description:
"""
from whetstone.core.entry import Entry
from jingxinting.biz_base_data.basic_index import BasicIndex


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    basic_index = BasicIndex(entry)
    basic_index.execute()


def post_check(entry: Entry):
    return True

#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: graph_main
:Author: xufeng@bbdservice.com 
:Date: 2021-05-12 7:07 PM
:Version: v.1.0
:Description:
"""
from whetstone.core.entry import Entry
from scwgj.graph_subject_data.graph_facade import DataExtractFacade
from scwgj.proj_common.component_conf_check_util import ComponentCheckUtil


def pre_check(entry: Entry):

    # TODO: add conf
    if not ComponentCheckUtil.check_mysql():
        entry.logger.error("error mysql conf")
        return False

    if not ComponentCheckUtil.check_es():
        entry.logger.error("error es config")
        return False

    return True


def main(entry: Entry):
    data_facade = DataExtractFacade(entry)
    data_facade.execute()


def post_check(entry: Entry):
    return True

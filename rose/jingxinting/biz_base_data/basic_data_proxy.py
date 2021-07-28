#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: basic_data_util
:Author: xufeng@bbdservice.com 
:Date: 2021-06-03 2:13 PM
:Version: v.1.0
:Description:
"""
from pyspark.sql.dataframe import DataFrame

from whetstone.core.entry import Entry
from jingxinting.biz_base_data.basic_index import BasicIndex


class BasicDataProxy:

    def __init__(self, entry: Entry):
        self._basic_index = BasicIndex(entry)

    def get_qyxx_basic_df(self) -> DataFrame:
        return self._basic_index.get_qyxx_basic_df()

    def get_patent_info_df(self) -> DataFrame:
        return self._basic_index.get_patent_info_df()

    def get_recruit_info_df(self) -> DataFrame:
        return self._basic_index.get_recruit_info_df()

    def get_park_info_df(self) -> DataFrame:
        return self._basic_index.get_park_info_df()

    def get_tags_info_df(self) -> DataFrame:
        return self._basic_index.get_company_tag_info()

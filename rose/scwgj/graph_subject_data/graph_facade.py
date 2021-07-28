#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: graph_facade
:Author: xufeng@bbdservice.com 
:Date: 2021-05-12 7:12 PM
:Version: v.1.0
:Description:
"""
from whetstone.core.entry import Entry
from scwgj.proj_common.single_util import SingletonClass
from scwgj.graph_subject_data.node_1_6 import *
from scwgj.graph_subject_data.node_7_13 import *
from scwgj.graph_subject_data.edge_1_11 import *
from scwgj.graph_subject_data.edge_12_22 import *
from scwgj.graph_subject_data.edge_23_32 import *


class DataExtractFacade(metaclass=SingletonClass):

    def __init__(self, entry: Entry):
        self.entry = entry
        self.logger = self.entry.logger
        self.spark = self.entry.spark
        self.extractors = self._get_all_table_extractor()

    def execute(self):
        # 为提供并行度，后续可以根据传入参数来选择执行哪一块
        for extractor in self.extractors:
            extractor.execute()

    def _get_all_table_extractor(self):
        return [
            VertexCorp(entry=self.entry),  # node - 1
            VertexOvscorp(entry=self.entry),  # node - 2

            EdgeCorpApplyBranch(entry=self.entry),  # edge - 1
            EdgeBranchDfxloanCorp(entry=self.entry),  # edge - 4
            EdgeCorpTradeCustom(entry=self.entry),  # edge - 23
            EdgeCustomRegisterMerch(entry=self.entry),  # edge - 24
            EdgeCorpHasFxaccount(entry=self.entry),  # edge - 25
            EdgeCorpHasRmbaccount(entry=self.entry),  # edge - 26
        ]

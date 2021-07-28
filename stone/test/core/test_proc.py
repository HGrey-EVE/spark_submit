#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-13 15:26
"""
from whetstone.core.proc import ProcInfo


def test_proc_1():
    assert isinstance(ProcInfo(project="project0", module="module1", env=""), ProcInfo)

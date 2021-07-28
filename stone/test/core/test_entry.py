#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-13 15:23
"""
import pytest

from whetstone.core.entry import Entry


def test_entry_1():
    assert isinstance(Entry().build_logger(None).build_cfg_mgr(None).build_spark(None), Entry)

def test_entry_2():
    with pytest.raises(AttributeError):
        assert isinstance(Entry().build_logger(None).build_cfg_mgr(None).build_spark(None).build_version(None), Entry)
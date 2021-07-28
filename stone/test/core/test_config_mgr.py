#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-13 11:02
"""
import collections
import os
from configparser import ConfigParser
from uuid import uuid4

from whetstone.common.settings import Setting
from whetstone.core.config_mgr import ConfigItem, parser_2_items, Config, ConfigManager
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo


def test_config_item_1():
    i = ConfigItem(section="section", key="key", value="value")
    assert str(i) == "section key value"


def test_parser_2_items_1():
    parser = ConfigParser()
    parser.add_section("section")
    parser.set(section="section", option="key", value="value")
    i = parser_2_items(parser)
    assert isinstance(i, collections.Sequence)
    assert isinstance(i[0], ConfigItem)
    assert str(i[0]) == "section key value"


def test_config_1():
    settings = Setting()
    proc = ProcInfo(project="project0", module="module1", env="dev", version="20210501", debug=True, script="module11")

    c = Config(PathManager.get_project_conf_path(proc, settings))
    assert len(c.items) == 19
    assert isinstance(c.items, collections.Sequence)
    assert isinstance(c, collections.Iterable)

    uid = str(uuid4())
    c.set(section="spark-submit", option="kerberos_user", value=uid)
    assert c.get("spark-submit", "kerberos_user") == uid

    for i in c:
        assert isinstance(i, ConfigItem)

    assert len(list((i for i in c if i.section == "spark-submit" and i.key == "kerberos_user" and i.value == uid))) == 1

    c2 = Config(PathManager.get_module_conf_path(proc, settings))
    assert len(c2.items) == 11

    assert c.get("spark-submit-opt", "queue") is None
    assert c2.get("spark-submit-opt", "queue") == "root.users.bbders"
    c4 = c.merge(c2)
    assert c4 is not c
    assert c4.get("spark-submit-opt", "queue") == "root.users.bbders"

    uid1 = str(uuid4())
    uid2 = str(uuid4())
    uid3 = str(uuid4())
    c4.set(uid1, uid2, uid3)
    c4.set("spark-submit-opt", "queue", uid3)
    assert c4.get(uid1, uid2) == uid3
    assert c4.get("spark-submit-opt", "queue") == uid3


def test_cfg_mgr_1():
    settings = Setting()
    proc = ProcInfo(project="project0", module="module1", env="dev", version="20210501", debug=True, script="module11")

    cfg_mgr = ConfigManager(proc=proc, settings=settings)
    assert isinstance(cfg_mgr.config, Config)
    assert isinstance(cfg_mgr.project, Config)
    assert isinstance(cfg_mgr.module, Config)
    assert cfg_mgr.get("spark-submit-opt", "queue") == "root.users.bbders"

    assert cfg_mgr.hdfs.get_input_path("biz", "path1") == os.path.join("/user/XXX/", "20210501", "input", "a/b/c1")
    assert cfg_mgr.hdfs.get_tmp_path("biz", "path1") == os.path.join("/user/XXX/", "20210501", "tmp", "a/b/c1")
    assert cfg_mgr.hdfs.get_result_path("biz", "path1") == os.path.join("/user/XXX/", "20210501", "result", "a/b/c1")
    assert cfg_mgr.hdfs.get_fixed_path("biz", "path1") == os.path.join("/user/XXX/", "a/b/c1")

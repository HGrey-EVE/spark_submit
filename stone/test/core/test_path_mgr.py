#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-13 14:05
"""
import collections
import os
from unittest import mock

from whetstone.common.settings import Setting
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo


def test_path_mgr_1():
    spark_source_root_path = "/data8/nationalcredit/spark_plan_zero/rose"
    framework_source_folder = "/data8/nationalcredit/spark_plan_zero/stone/src"

    settings = Setting()
    settings.get_spark_source_root_path = mock.Mock(return_value=spark_source_root_path)
    settings.get_framework_source_folder = mock.Mock(return_value=framework_source_folder)

    proc = ProcInfo(project="project0", module="module1", env="dev", version="20210501", debug=True, script="module11")
    cfg_name = PathManager.get_application_conf_name(proc=proc, settings=settings)
    assert cfg_name == "application.dev.conf"

    proc = ProcInfo(project="project0", module="module1", env="", version="20210501", debug=True, script="module11")
    cfg_name = PathManager.get_application_conf_name(proc=proc, settings=settings)
    assert cfg_name == "application.conf"

    assert PathManager.get_project_conf_path(proc=proc, settings=settings) == os.path.join(spark_source_root_path,
                                                                                           "project0",
                                                                                           "conf",
                                                                                           "application.conf")

    assert PathManager.get_module_conf_path(proc=proc, settings=settings) == os.path.join(spark_source_root_path,
                                                                                          "project0",
                                                                                          "module1",
                                                                                          "conf",
                                                                                          "application.conf")

    lib_paths = PathManager.get_application_lib_path(proc=proc, settings=settings)
    assert isinstance(lib_paths, collections.Sequence)
    assert len(lib_paths) == 2
    assert os.path.join(spark_source_root_path, "project0", "lib") in lib_paths
    assert os.path.join(spark_source_root_path, "project0", "module1", "lib") in lib_paths

    assert os.path.join(spark_source_root_path, "project0") == PathManager.get_project_root_path(proc=proc,
                                                                                                 settings=settings)

    assert os.path.join(spark_source_root_path, "project0", "module1") == PathManager.get_project_module_path(proc=proc,
                                                                                                              settings=settings)

    assert ".".join(["project0", "module1", "module11"]) == PathManager.get_module_py_path(proc=proc)
    proc = ProcInfo(project="project0", module="module1", env="", version="20210501", debug=True, script="")
    assert ".".join(["project0", "module1", "module1"]) == PathManager.get_module_py_path(proc=proc)

#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-13 16:11
"""
import os

import pytest

from whetstone.common.exception import FriendlyException
from whetstone.common.settings import Setting
from whetstone.core.config_mgr import ConfigManager
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo
from whetstone.core.runner import get_runner_path
from whetstone.core.submiter import package_py_files, check_jar_with_folder, get_spark_submit_cmd


def test_package_py_files():
    settings = Setting()

    _proc = ProcInfo(project="project0", module="module1", env="dev", version="20210501", debug=True, script="module11")

    zip_paths = package_py_files(_proc, settings)
    assert os.path.join(settings.get_spark_source_root_path(), "tmp", "project0.zip") in zip_paths
    assert os.path.join(settings.get_spark_source_root_path(), "tmp",
                        f"{settings.get_framework_name()}.zip") in zip_paths


def test_check_jar_with_folder():
    assert not check_jar_with_folder("/a/b", "c.jar")
    assert check_jar_with_folder(os.path.dirname(os.path.realpath(__file__)), __file__) == __file__


def test_get_spark_submit_cmd():
    _settings = Setting()
    _proc = ProcInfo(project="project0", module="module1", env="dev", version="20210501", debug=True, script="module11")
    _cfg_mgr = ConfigManager(_proc, _settings)
    cmd = get_spark_submit_cmd(_proc, _cfg_mgr, _settings, get_runner_path())
    cmd1 = " ".join([
        "kinit /a/b/user.kerberos test_user",
        "/usr/local/bin/spark2-submit --master yarn --deploy-mode client --queue root.users.bbders",
        "--jars",
        f"{os.path.join(PathManager.get_application_lib_path(_proc, _settings)[0], 'elasticsearch-spark-20_2.11-6.4.1.jar')},"
        f"{os.path.join(PathManager.get_application_lib_path(_proc, _settings)[0], 'elasticsearch-hadoop-mr-6.5.2.jar')}",
        "--py-files",
        f"{os.path.join(_settings.get_application_tmp_root_path(), 'whetstone.zip')},"
        f"{os.path.join(_settings.get_application_tmp_root_path(), 'project0.zip')}",
        "--conf spark.pyspark.driver.python=/opt/anaconda3/bin/python",
        "--conf spark.pyspark.python=/opt/anaconda3/bin/python",
        f"{get_runner_path()}",
        "-p project0 -m module1 -e dev -d -t module11 -v 20210501"
    ])
    assert cmd1 == str(cmd)

def test_get_spark_submit_cmd_2():
    _settings = Setting()
    _proc = ProcInfo(project="project0", module="module2", env="dev", version="20210501", debug=True, script="moduleXXX")
    _cfg_mgr = ConfigManager(_proc, _settings)
    cmd = get_spark_submit_cmd(_proc, _cfg_mgr, _settings, get_runner_path())
    cmd1 = " ".join([
        "kinit /a/b/user.kerberos test_user",
        "/usr/local/bin/spark2-submit --master yarn --deploy-mode cluster --queue root.users.bbders",
        "--jars",
        f"{os.path.join(PathManager.get_application_lib_path(_proc, _settings)[0], 'elasticsearch-spark-20_2.11-6.4.1.jar')},"
        f"{os.path.join(PathManager.get_application_lib_path(_proc, _settings)[0], 'elasticsearch-hadoop-mr-6.5.2.jar')}",
        "--py-files",
        f"{os.path.join(_settings.get_application_tmp_root_path(), 'whetstone.zip')},"
        f"{os.path.join(_settings.get_application_tmp_root_path(), 'project0.zip')}",
        "--conf spark.pyspark.driver.python=/opt/anaconda3/bin/python",
        "--conf spark.pyspark.python=/opt/anaconda3/bin/python",
        f"--conf spark.yarn.dist.archives={os.path.join(_settings.get_application_tmp_root_path(), 'project0-N.zip')}#project-project0",
        f"{get_runner_path()}",
        "-p project0 -m module2 -e dev -d -t moduleXXX -v 20210501"
    ])
    assert cmd1 == str(cmd)


def test_get_spark_submit_cmd_3():
    _settings = Setting()
    _proc = ProcInfo(project="project0", module="module2", env="dev", version="20210501", debug=True, script="moduleXXX")
    _cfg_mgr = ConfigManager(_proc, _settings)
    _cfg_mgr.config.set( "spark-submit-conf", "spark.yarn.dist.archives", "anything")
    cmd = get_spark_submit_cmd(_proc, _cfg_mgr, _settings, get_runner_path())
    cmd1 = " ".join([
        "kinit /a/b/user.kerberos test_user",
        "/usr/local/bin/spark2-submit --master yarn --deploy-mode cluster --queue root.users.bbders",
        "--jars",
        f"{os.path.join(PathManager.get_application_lib_path(_proc, _settings)[0], 'elasticsearch-spark-20_2.11-6.4.1.jar')},"
        f"{os.path.join(PathManager.get_application_lib_path(_proc, _settings)[0], 'elasticsearch-hadoop-mr-6.5.2.jar')}",
        "--py-files",
        f"{os.path.join(_settings.get_application_tmp_root_path(), 'whetstone.zip')},"
        f"{os.path.join(_settings.get_application_tmp_root_path(), 'project0.zip')}",
        "--conf spark.pyspark.driver.python=/opt/anaconda3/bin/python",
        "--conf spark.pyspark.python=/opt/anaconda3/bin/python",
        f"--conf spark.yarn.dist.archives=anything,{os.path.join(_settings.get_application_tmp_root_path(), 'project0-N.zip')}#project-project0",
        f"{get_runner_path()}",
        "-p project0 -m module2 -e dev -d -t moduleXXX -v 20210501"
    ])
    assert cmd1 == str(cmd)

def test_get_spark_submit_cmd_4():
    with pytest.raises(FriendlyException):
        _settings = Setting()
        _proc = ProcInfo(project="project0", module="module2", env="dev", version="20210501", debug=True, script="moduleXXX")
        _cfg_mgr = ConfigManager(_proc, _settings)
        _cfg_mgr.config.set( "spark-submit-conf", "spark.yarn.dist.archives", "anything")
        # add invalid key value to the build-in config section
        _cfg_mgr.config.set( "spark-submit", "spark_home", "anything")

        cmd = get_spark_submit_cmd(_proc, _cfg_mgr, _settings, get_runner_path())
        cmd1 = " ".join([
            "kinit /a/b/user.kerberos test_user",
            "/usr/local/bin/spark2-submit --master yarn --deploy-mode cluster --queue root.users.bbders",
            "--jars",
            f"{os.path.join(PathManager.get_application_lib_path(_proc, _settings)[0], 'elasticsearch-spark-20_2.11-6.4.1.jar')},"
            f"{os.path.join(PathManager.get_application_lib_path(_proc, _settings)[0], 'elasticsearch-hadoop-mr-6.5.2.jar')}",
            "--py-files",
            f"{os.path.join(_settings.get_application_tmp_root_path(), 'whetstone.zip')},"
            f"{os.path.join(_settings.get_application_tmp_root_path(), 'project0.zip')}",
            "--conf spark.pyspark.driver.python=/opt/anaconda3/bin/python",
            "--conf spark.pyspark.python=/opt/anaconda3/bin/python",
            f"--conf spark.yarn.dist.archives=anything,{os.path.join(_settings.get_application_tmp_root_path(), 'project0-N.zip')}#project-project0",
            f"{get_runner_path()}",
            "-p project0 -m module2 -e dev -d -t moduleXXX -v 20210501"
        ])
        assert cmd1 == str(cmd)
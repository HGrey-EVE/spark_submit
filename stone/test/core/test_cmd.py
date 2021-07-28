#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-12 10:54
"""
from whetstone.core.cmd import Command
from whetstone.core.proc import ProcInfo


def test_cmd_1():
    c = Command()
    submit = "/opt/spark-2.4.0/bin/spark-submit"
    assert c.build_spark_submit_path(submit) is c
    assert submit == str(c)

    submit_target_script = "/a/b/c.py"
    assert c.build_target_script_path(submit_target_script) is c
    assert " ".join([submit, submit_target_script]) == str(c)

    py_files_1 = ["/a/b/common.zip", "/a/b/common2.zip"]
    assert  c.build_py_files(*py_files_1) is c
    py_files_str = ",".join(py_files_1)
    assert " ".join([submit, "--py-files", py_files_str, submit_target_script]) == str(c)

    py_files_2 = ["/a/b/common3.zip", "/a/b/common4.zip"]
    py_files_2_str = ",".join(py_files_2)
    assert c.build_py_files(py_files_2_str) is c
    py_files_1.extend(py_files_2)
    py_files_str = ",".join(py_files_1)
    assert " ".join([submit, "--py-files", py_files_str, submit_target_script]) == str(c)

    p = ProcInfo(project="", module="", env="", version="", debug=False, script="")
    assert c.build_proc_info(p) is c
    assert " ".join([submit, "--py-files", py_files_str, submit_target_script]) == str(c)

    p = ProcInfo(project="demo", module="module1", env="dev", version="20210501", debug=True, script="target")
    assert c.build_proc_info(p) is c
    proc = "-p demo -m module1 -e dev -d -t target -v 20210501"
    assert " ".join([submit, "--py-files", py_files_str, submit_target_script, proc]) == str(c)
    jars1 = ["/a/b/j.jar", "/a/b/j2.jar"]
    assert c.build_jars(*jars1) is c
    jar_str = ",".join(jars1)
    assert " ".join([submit, "--jars", jar_str, "--py-files", py_files_str, submit_target_script, proc]) == str(c)

    jars2 = ["/a/b/j3.jar", "/a/b/j4.jar"]
    jar_str_2 = ",".join(jars2)
    assert c.build_jars(jar_str_2) is c
    jars1.extend(jars2)
    jar_str = ",".join(jars1)
    assert " ".join([submit, "--jars", jar_str, "--py-files", py_files_str, submit_target_script, proc]) == str(c)

    kerberos_file = "/a/b/user.kerberos"
    assert c.build_kerberos_file(kerberos_file) is c
    assert " ".join([submit, "--jars", jar_str, "--py-files", py_files_str, submit_target_script, proc]) == str(c)

    kerberos_user = "test_user"
    assert c.build_kerberos_user(kerberos_user) is c
    kerberos = " ".join(["kinit", kerberos_file, kerberos_user])
    assert " ".join(
        [kerberos, submit, "--jars", jar_str, "--py-files", py_files_str, submit_target_script, proc]) == str(c)

    opts = {
        "master": "yarn",
        "deploy-mode": "client",
        "queue": "project.queue"
    }

    opts_list = []
    for key, value in opts.items():
        assert c.build_spark_submit_opts(key, value) is c
        opts_list.append(f"--{key}")
        opts_list.append(value)
    opts_str = " ".join(opts_list)
    assert " ".join(
        [kerberos, submit, opts_str, "--jars", jar_str, "--py-files", py_files_str, submit_target_script,
         proc]) == str(c)

    confs = {
        "spark.executor.memory": "20g",
        "spark.executor.cores": "6",
        "spark.default.parallelism": "1000",
        "spark.executor.instances": "40",
        "spark.sql.shuffle.partitions": "1000"
    }

    conf_list = []
    for key, value in confs.items():
        assert c.build_spark_submit_conf(key, value) is c
        conf_list.append(f"--conf {key}={value}")
    conf_str = " ".join(conf_list)
    assert " ".join(
        [kerberos, submit, opts_str, "--jars", jar_str, "--py-files", py_files_str, conf_str, submit_target_script,
         proc]) == str(c)

    print(c)
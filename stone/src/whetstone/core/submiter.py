#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-12-20 16:51
"""
import os
from collections import MutableMapping

from whetstone.common.exception import FriendlyException
from whetstone.common.settings import Setting
from whetstone.core.cmd import Command
from whetstone.core.config_mgr import ConfigManager
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo


def package_py_files(proc: ProcInfo, settings: Setting):
    if not os.path.exists(settings.get_application_tmp_root_path()):
        os.system(f"mkdir {settings.get_application_tmp_root_path()}")
    file1 = f"{os.path.join(settings.get_application_tmp_root_path(), settings.get_framework_name() + '.zip')}"
    cmd = f"cd {settings.get_framework_source_folder()}; zip -r {file1} ./{settings.get_framework_name()}"
    print(cmd)
    os.system(cmd)
    file2 = f"{os.path.join(settings.get_application_tmp_root_path(), proc.project + '.zip')}"
    cmd = f"cd {settings.get_spark_source_root_path()}; zip -r {file2} ./{proc.project}"
    print(cmd)
    os.system(cmd)
    return file1, file2


def get_spark_submit_cmd(proc: ProcInfo, cfg_mgr: ConfigManager, settings: Setting, entry_script_path: str):
    default_conf_cmd_build_mapping = {
        settings.get_default_conf_spark_submit_conf_section_name(): "build_spark_submit_conf",
        settings.get_default_conf_spark_submit_opt_section_name(): "build_spark_submit_opts",
        settings.get_default_conf_spark_submit_jar_section_name(): "build_jars",
        settings.get_default_conf_spark_submit_section_name(): {
            "spark_submit_path": "build_spark_submit_path",
            # "spark_home": "build_spark_home",
            "kerberos_user": "build_kerberos_user",
            "kerberos_file": "build_kerberos_file",
        }
    }
    cmd = Command()
    for item in cfg_mgr.config.items:
        if item.section in default_conf_cmd_build_mapping:
            build_method = default_conf_cmd_build_mapping[item.section]
            if isinstance(build_method, (str,)):
                if item.section == settings.get_default_conf_spark_submit_jar_section_name():
                    jar_paths = []
                    missing = []
                    names = [x.strip() for x in item.value.split(",") if x.strip()]
                    for file_name in names:
                        folders = PathManager.get_application_lib_path(proc, settings)
                        for folder in folders:
                            jar_path = check_jar_with_folder(folder, file_name)
                            jar_paths.append(jar_path) if jar_path else missing.append(file_name)
                    if len(names) != len(jar_paths):
                        raise FriendlyException(f"Do not find all jar path {missing}")
                    else:
                        cmd.build_jars(*jar_paths)
                else:
                    # direct call the cmd build method with key and value
                    func = getattr(cmd, build_method)
                    func(item.key, item.value)
            elif issubclass(type(build_method), (MutableMapping,)):
                if item.key in build_method:
                    func = getattr(cmd, build_method[item.key])
                    func(item.value)
                else:
                    raise FriendlyException(
                        f"Dont find any cmd build method for config item {item.key} with section {item.section}")
            else:
                raise FriendlyException("Oops, should never print this information")
        else:
            pass
            # not cmd build conf item, continue
    cmd.build_target_script_path(entry_script_path)
    cmd.build_proc_info(proc)
    framework_zip, project_zip = package_py_files(proc, settings)
    if spark_cluster_mode(cfg_mgr, settings):
        cmd.patch_spark_cluster_mode(project_zip, proc)
    cmd.build_py_files(*[framework_zip, project_zip])
    return str(cmd)


def check_jar_with_folder(folder, jar):
    jar_path = os.path.join(folder, jar)
    if os.path.exists(jar_path):
        return jar_path
    else:
        return ""


def spark_cluster_mode(cfg_mgr: ConfigManager, settings: Setting):
    return bool(cfg_mgr.get(settings.get_default_conf_spark_submit_opt_section_name(), "deploy-mode") == "cluster")

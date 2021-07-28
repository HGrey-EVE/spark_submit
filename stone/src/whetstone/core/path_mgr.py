#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:File Name: path_mgr.py
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-12-20 16:51
:Version: v.1.0
"""
import os
import typing

from whetstone.common.settings import Setting
from whetstone.core.proc import ProcInfo


class PathManager:

    @staticmethod
    def get_application_conf_name(proc: ProcInfo, settings: Setting) -> str:
        file_name_parts = settings.get_default_conf_file_name().split(".")
        file_name_parts.insert(-1, proc.env) if proc.env else None
        return ".".join(file_name_parts)

    @staticmethod
    def get_application_lib_path(proc: ProcInfo, settings: Setting) -> typing.Tuple[str]:
        return (
            os.path.join(PathManager.get_project_root_path(proc, settings), settings.get_default_lib_folder_name()),
            os.path.join(PathManager.get_project_module_path(proc, settings), settings.get_default_lib_folder_name()),
        )

    @staticmethod
    def get_project_root_path(proc: ProcInfo, settings: Setting) -> str:
        return os.path.join(settings.get_spark_source_root_path(), proc.project)

    @staticmethod
    def get_project_module_path(proc: ProcInfo, settings: Setting) -> str:
        return os.path.join(PathManager.get_project_root_path(proc, settings), proc.module)

    @staticmethod
    def get_module_py_path(proc: ProcInfo) -> str:
        return ".".join([proc.project, proc.module, proc.script or proc.module])

    @staticmethod
    def get_project_conf_path(proc: ProcInfo, settings: Setting) -> str:
        return os.path.join(PathManager.get_project_root_path(proc, settings),
                            settings.get_default_conf_folder_name(),
                            PathManager.get_application_conf_name(proc, settings)
                            )

    @staticmethod
    def get_project_tmp_path(proc: ProcInfo, settings: Setting) -> str:
        return os.path.join(PathManager.get_project_root_path(proc, settings),
                            "tmp")

    @staticmethod
    def get_module_conf_path(proc: ProcInfo, settings: Setting) -> str:
        return os.path.join(PathManager.get_project_module_path(proc, settings),
                            settings.get_default_conf_folder_name(),
                            PathManager.get_application_conf_name(proc, settings)
                            )

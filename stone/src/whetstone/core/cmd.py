#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-02-03 17:49
"""
import os
import typing

from whetstone.core.proc import ProcInfo


class Command:

    def __init__(self):
        self._spark_submit_path = ""
        # self._spark_home = ""
        self._target_script_path = ""
        self._kerberos_user = ""
        self._kerberos_file = ""
        self._spark_submit_opts: typing.List[typing.Sequence[str]] = []
        self._spark_submit_conf: typing.List[typing.Sequence[str]] = []
        self._jars: typing.List[str] = []
        self._py_files: typing.List[str] = []

        self._proc: ProcInfo = None

    def _command_component(self):
        yield f"kinit {self._kerberos_file} {self._kerberos_user}" if self._kerberos_file and self._kerberos_user else ""
        yield f"{self._spark_submit_path}"

        for key, value in self._spark_submit_opts:
            yield f"--{key} {value}"

        if self._jars:
            yield f"--jars {','.join(self._jars)}"

        if self._py_files:
            yield f"--py-files {','.join(self._py_files)}"

        for key, value in self._spark_submit_conf:
            yield f"--conf {key}={value}"

        if self._proc:
            yield f"{self._target_script_path}"
            yield f"-p {self._proc.project}" if self._proc.project else ""
            yield f"-m {self._proc.module}" if self._proc.module else ""
            yield f"-e {self._proc.env}" if self._proc.env else ""
            yield f"-d" if self._proc.debug else ""
            yield f"-t {self._proc.script}" if self._proc.script else ""
            yield f"-v {self._proc.version}" if self._proc.version else ""
        else:
            yield f"{self._target_script_path}"

    def __str__(self):
        return " ".join([cmd for cmd in self._command_component() if cmd])

    def build_spark_submit_path(self, spark_submit_path: str):
        if spark_submit_path:
            self._spark_submit_path = spark_submit_path
        return self

    # def build_spark_home(self, spark_home: str):
    #     if spark_home:
    #         self._spark_home = spark_home
    #     return self

    def build_target_script_path(self, target_script_path: str):
        if target_script_path:
            self._target_script_path = target_script_path
        return self

    def build_kerberos_file(self, kerberos_file: str):
        if kerberos_file:
            self._kerberos_file = kerberos_file
        return self

    def build_kerberos_user(self, kerberos_user: str):
        if kerberos_user:
            self._kerberos_user = kerberos_user
        return self

    def build_spark_submit_opts(self, key, value):
        self._spark_submit_opts.append([key, value])
        return self

    def build_spark_submit_conf(self, key, value):
        self._spark_submit_conf.append([key, value])
        return self

    def build_jars(self, *jars):
        self._jars.extend(jars)
        return self

    def build_py_files(self, *files):
        self._py_files.extend(files)
        return self

    def build_proc_info(self, proc: ProcInfo):
        self._proc = proc
        return self

    def patch_spark_cluster_mode(self, project_zip, proc: ProcInfo):
        archives = [item for item in self._spark_submit_conf if item[0] == "spark.yarn.dist.archives"]
        paths = list(os.path.splitext(project_zip))
        paths[0] = paths[0] + "-N"
        copy_project_zip = f"{paths[0]}{paths[1]}"
        if os.path.exists(project_zip):
            os.system(f"cp {project_zip} {copy_project_zip}")
        if archives:
            archives[0][1] = f"{archives[0][1]},{copy_project_zip}#project-{proc.project}"
        else:
            self.build_spark_submit_conf("spark.yarn.dist.archives",
                                         f"{copy_project_zip}#project-{proc.project}")

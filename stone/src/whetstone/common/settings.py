#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-12-20 16:51
"""
import os


class Setting:

    # source setting
    @staticmethod
    def get_spark_source_root_path():
        spark_source_root_path = os.path.join(
            os.path.dirname(  # spark_plan_zero
                os.path.dirname(  # whetstone
                    os.path.dirname(  # src
                        os.path.dirname(  # whetstone
                            os.path.dirname(  # common
                                os.path.realpath(__file__)))))),
            "rose")
        return spark_source_root_path

    @staticmethod
    def get_framework_source_folder():
        return os.path.dirname(  # src
            os.path.dirname(  # whetstone
                os.path.dirname(  # common
                    os.path.realpath(__file__))))

    @staticmethod
    def get_application_tmp_root_path():
        return os.path.join(Setting.get_spark_source_root_path(), "tmp")

    @staticmethod
    def get_framework_name():
        return "whetstone"

    # configuration setting

    @staticmethod
    def get_default_conf_folder_name():
        return "conf"

    @staticmethod
    def get_default_lib_folder_name():
        return "lib"

    @staticmethod
    def get_default_conf_file_name():
        return "application.conf"

    @staticmethod
    def get_default_conf_spark_submit_opt_section_name():
        return "spark-submit-opt"

    @staticmethod
    def get_default_conf_spark_submit_conf_section_name():
        return "spark-submit-conf"

    @staticmethod
    def get_default_conf_spark_conf_section_name():
        return "spark-conf"

    @staticmethod
    def get_default_conf_spark_submit_section_name():
        return "spark-submit"

    @staticmethod
    def get_default_conf_spark_submit_jar_section_name():
        return "spark-submit-jar"

    @staticmethod
    def get_default_index_module_func():
        return [
            "main",
            "pre_check",
            "post_check"
        ]

    # logging setting
    @staticmethod
    def get_application_log_root_path():
        return os.path.join(Setting.get_spark_source_root_path(), "logs")

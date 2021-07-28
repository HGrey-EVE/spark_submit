#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: common_util
:Author: xufeng@bbdservice.com 
:Date: 2021-04-16 11:39 AM
:Version: v.1.0
:Description: common python util
    date process
    exec sub process
"""
import os
import subprocess
import tempfile

from pyspark import RDD


def exec_shell(command: str, description: str = None):
    """
    :param command:
    :param description:
    :return:
    """
    out_temp = tempfile.SpooledTemporaryFile()
    file_no = out_temp.fileno()
    try:
        command = command.strip()
        if description:
            print(description + command)
        else:
            print(command)
        p = subprocess.Popen(command, stdout=file_no, stderr=file_no, shell=True)
        p.communicate()
        out_temp.seek(0)
        for line in out_temp.readlines():
            print(line)
    finally:
        if out_temp:
            out_temp.close()


def ensure_dir_path(dir_or_file_path):
    dir_path = os.path.dirname(dir_or_file_path)
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path, exist_ok=True)
        except OSError:
            print(f'handle dir path failed:{dir_path}')
            raise Exception('handle dir path failed')


def save_text_to_hdfs(rdd: RDD, path, repartition_number=None, force_repartition=False):
    print(f'====begin remove old text file:{path}')
    os.system(f"hadoop fs -rm -r {path}")

    if not repartition_number:
        rdd.saveAsTextFile(path)
        return

    if force_repartition:
        rdd.repartition(repartition_number).saveAsTextFile(path)
        return

    current_number = rdd.getNumPartitions()

    if current_number == repartition_number:
        rdd.saveAsTextFile(path)

    elif current_number > repartition_number:
        rdd.coalesce(repartition_number).saveAsTextFile(path)

    else:
        rdd.repartition(repartition_number).saveAsTextFile(path)

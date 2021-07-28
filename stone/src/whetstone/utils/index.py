#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-06-04 13:46
"""
import os
import typing

from pyspark.sql import DataFrame, SparkSession

from whetstone.common.exception import FriendlyException

__all__ = ["index_join_merge"]


def _recursion_index_merge(spark: SparkSession, target_df: DataFrame, target_columns: typing.List[str],
                           index_df_list: typing.List[DataFrame], index_columns: typing.List[str],
                           join_columns: typing.List[str],
                           hdfs_tmp_path,
                           slice_step_num=5, partition_num=None, level=1) -> DataFrame:
    if not all([target_columns, index_columns, join_columns]):
        raise FriendlyException("Dataframe columns should specific for join and select operation")

    slice_step_num = slice_step_num or 5
    index_df_temp_list = [index_df_list[i:i + slice_step_num] for i in range(0, len(index_df_list), slice_step_num)]
    group_rst_df_list = _join_merge(spark,
                                    target_df,
                                    index_df_temp_list,
                                    join_columns,
                                    hdfs_tmp_path,
                                    partition_num,
                                    level)

    if len(group_rst_df_list) < 1:
        return target_df
    elif len(group_rst_df_list) == 1:
        return group_rst_df_list[0].join(target_df, join_columns).select(list(set(target_columns + index_columns)))
    else:
        return _recursion_index_merge(spark, target_df, target_columns, group_rst_df_list, index_columns,
                                      join_columns, hdfs_tmp_path, slice_step_num, partition_num, level + 1)


def _join_merge(spark, target_df, index_df_temp_list,
                join_columns,
                hdfs_tmp_path,
                partition_num,
                level):
    group_rst_df_list = []
    for idx, group_df_list in enumerate(index_df_temp_list):
        rst_df = target_df.select(join_columns)
        for index_df in group_df_list:
            rst_df = rst_df.join(index_df, join_columns, 'left').drop_duplicates(join_columns)
            rst_df = rst_df.repartition(partition_num) if partition_num else rst_df
        rst_df.write.parquet(os.path.join(hdfs_tmp_path, f"piece_{level}_{idx}"), mode="overwrite")
        group_rst_df_list.append(spark.read.parquet(os.path.join(hdfs_tmp_path, f"piece_{level}_{idx}")))
    return group_rst_df_list


def index_join_merge(spark: SparkSession, target_df: DataFrame, target_columns: typing.List[str],
                     index_df_list: typing.List[DataFrame], index_columns: typing.List[str],
                     join_columns: typing.List[str],
                     hdfs_tmp_path,
                     slice_step_num=5, partition_num=None) -> DataFrame:
    return _recursion_index_merge(spark, target_df, target_columns, index_df_list, index_columns,
                                  join_columns, hdfs_tmp_path, slice_step_num, partition_num, 1)


def _XX_merge(spark, target, index_df_temp_list, join_columns, hdfs_tmp_path, partition_num, level):
    group_rst_df_list = []
    for idx, group_df_list in enumerate(index_df_temp_list):
        rst_df = target.select(join_columns)
        for index_df in group_df_list:
            rst_df = rst_df.join(index_df, join_columns, 'left').drop_duplicates(join_columns)
            rst_df = rst_df.repartition(partition_num) if partition_num else rst_df
        rst_df.write.parquet(os.path.join(hdfs_tmp_path, f"piece_{level}_{idx}"), mode="overwrite")
        group_rst_df_list.append(spark.read.parquet(os.path.join(hdfs_tmp_path, f"piece_{level}_{idx}")))
    return group_rst_df_list

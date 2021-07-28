#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: data_output_util
:Author: xufeng@bbdservice.com 
:Date: 2021-06-02 6:37 PM
:Version: v.1.0
:Description:
    数据存储工具类
        基础数据直接存储到hdfs和当前version对应目录,并将该数据读取出来用json的方式写入到datashare推送目录
        统计类数据
            是首次计算的和基础数据处理方式一样
            不是首次计算的，取出本次计算的当前version的数据，并和最后一次计算的数据做联合作为最后输出
"""
import os
import traceback

from pyspark.sql import DataFrame

from whetstone.core.entry import Entry
from whetstone.common.exception import FriendlyException
from jingxinting.proj_common.common_util import check_hdfs_path_exists, save_text_to_hdfs
from jingxinting.proj_common.date_util import DateUtils
from jingxinting.proj_common.json_util import JsonUtils


class ResultOutputUtil:

    def __init__(self,
                 entry: Entry,
                 table_name,
                 month_field='static_month',
                 month_value_fmt='%Y%m',
                 is_statistic=True,
                 need_write_result=True):
        """
        :param entry:
        :param table_name: 输出表的表名
        :param month_field: 统计中代表月份的字段
        :param month_value_fmt: 月份字段值的格式
        :param is_statistic: 是否为统计表(分基础表　和　统计表，二者的处理方式有区别)
        :param need_write_result: 统计前是否已经将df数据写到结果目录(只有基本表会写，统计表调该方法之前不能写到结果目录)
        """
        self.entry = entry
        self.spark = self.entry.spark
        self.logger = self.entry.logger
        self.version = self.entry.version
        self.debug = entry.cfg_mgr.get('exec-mode', 'debug')
        self.first_version = self._get_first_version()

        self.table_name = table_name
        self.month_field = month_field
        self.month_value_fmt = month_value_fmt
        self.is_statistic = is_statistic
        self.need_write_result = need_write_result

        self.result_path = self._get_result_path(self.table_name)
        self.data_share_path = self._get_data_share_path(self.table_name)

    def save(self, table_df: DataFrame = None):

        try:
            #  处理非统计数据(基础数据，和月份不挂钩)
            if not self.is_statistic:
                self._save_base_table(table_df)
                return

            # 处理统计数据
            self._save_statistic_table(table_df)
        except FriendlyException:
            raise
        except Exception:
            self.logger.error(f"save {self.table_name} failed: {traceback.format_exc()}")
            if not self.debug:
                raise

    def _save_base_table(self, table_df: DataFrame = None):
        if self.need_write_result and table_df:
            table_df.write.parquet(self.result_path, mode='overwrite')

        table_df = self.spark.read.parquet(self.result_path)
        self._dump_result_to_data_share(table_df)
        self.logger.info(f"save {self.table_name} success!")

    def _save_statistic_table(self, table_df: DataFrame):

        row = table_df.take(1)
        if not row:
            raise FriendlyException(f"table: {self.table_name} no data found!")

        # 是第一个版本的，将所有数据写入结果目录和 data share 目录
        if self.first_version == self.version:
            self._save_base_table(table_df)
            return

        # 找到前一个version,如果不存在前一个version,直接报错
        last_version = self._get_last_month_version()
        last_hdfs_path = self.result_path.replace(self.version, last_version)
        if not check_hdfs_path_exists(last_hdfs_path):
            raise FriendlyException(f"table {self.table_name} last month data not found!")

        # 取出上一个月计算的完整数据
        last_df = self.spark.read.parquet(last_hdfs_path)

        # 获取数据中当月数据的月份值
        current_month_value = self._get_current_month_value()

        # 提取当月数据并和以前的数据做union，写入 result path
        table_df\
            .filter(f"{self.month_field} = {current_month_value}")\
            .unionAll(last_df)\
            .write\
            .parquet(self.result_path, mode='overwrite')
        table_df = self.spark.read.parquet(self.result_path)

        # 提取 result path 中的数据,以　json 格式复制到 data share 推送目录
        self._dump_result_to_data_share(table_df)
        self.logger.info(f"save statistic table {self.table_name} success!")

    def _dump_result_to_data_share(self, table_df: DataFrame):
        rdd = table_df.rdd.map(lambda x: (JsonUtils.to_string(x.asDict())))
        save_text_to_hdfs(rdd, self.data_share_path)

    def _get_first_version(self):
        first_version = self.entry.cfg_mgr.get('biz', 'first_version')
        first_version = '202105' if not first_version else str(first_version)
        return first_version

    def _get_last_month_version(self) -> str:
        return DateUtils.add_date_2str_4str(self.version, in_fmt='%Y%m', out_fmt='%Y%m', months=-1)

    def _get_current_month_value(self) -> str:
        return DateUtils.date2str(DateUtils.str2date(self.version, '%Y%m'), fmt=self.month_value_fmt)

    def _get_result_path(self, table_name):
        base_path = self.entry.cfg_mgr.hdfs.get_result_path('hdfs-biz', 'path_result')
        return os.path.join(base_path, table_name)

    def _get_data_share_path(self, table_name):
        base_path = self.entry.cfg_mgr.hdfs.get_fixed_path('hdfs-biz', 'data_share_path')
        return os.path.join(base_path, table_name)

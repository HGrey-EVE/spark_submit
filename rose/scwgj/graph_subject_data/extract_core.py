#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: extract_core
:Author: xufeng@bbdservice.com 
:Date: 2021-05-07 2:32 PM
:Version: v.1.0
:Description: extract subject data and load to database
"""
import abc
import os
import traceback

from pyspark.sql import DataFrame
from pyspark.sql import functions as fun

from whetstone.common.exception import FriendlyException
from whetstone.core.entry import Entry
from scwgj.proj_common.mysql_util import get_mysql_instance
from scwgj.proj_common.enums import MySqlSection, TableRowOperation, MysqlDataChangeTableINfo
from scwgj.proj_common.hive_util import HiveUtil
from scwgj.proj_common.date_util import DateUtils
from scwgj.proj_common.json_util import JsonUtils
from scwgj.proj_common.common_util import exec_shell


class TableMetaData:

    def __init__(
        self,
        source_table_name: str = '',
        destination_table_name=None,
        destination_table_columns=None,
        primary_keys=None,
        has_update_data=True
    ):
        """
        :param source_table_name: hive中表的名称,只有一张源表的可以写源表名称，后续针对只有一张表的，
            如果只是提取同名字段会做工具类来处理数据，多张表的不用写
        :param destination_table_name: 写入mysql后，全量表的名称
        :param destination_table_columns: 写入mysql后，全量表的所有字段
        :param primary_keys: 生成的目标表的主键
        :param has_update_data:　是否存在变更数据　像边节点只有　id, start_node, end_node 的，就不存在变更数据
        """
        self._source_table_name = source_table_name
        self._destination_table_name = destination_table_name
        self._destination_table_columns = destination_table_columns
        self._primary_keys = primary_keys
        self._has_update_data = has_update_data

    @property
    def source_table_name(self):
        return self._source_table_name

    @property
    def destination_table_name(self):
        return self._destination_table_name

    @property
    def destination_table_columns(self):
        return self._destination_table_columns

    @property
    def primary_keys(self):
        return self._primary_keys

    @property
    def has_update_data(self):
        return self._has_update_data

    def __repr__(self):
        return ','.join(f'{key[1:]}: {getattr(self, key)}' for key in self.__dict__)


class GraphDataExtractTemplate(metaclass=abc.ABCMeta):

    def __init__(self, entry: Entry, exec_version=None):
        self.entry = entry
        self.logger = entry.logger
        self.spark = entry.spark
        self.table_name = None
        self.table_meta_data = self.meta_data()
        self.max_df_part_to_mysql = int(self.entry.cfg_mgr.get("graph-biz", "max_df_part_to_mysql"))
        self.mysql = get_mysql_instance(MySqlSection.MYSQL_DESTINATION, entry.cfg_mgr)
        self.exec_version = exec_version if exec_version else entry.version
        self.hive_dw_db_name = self.entry.cfg_mgr.get('hive-dw', 'database')
        self.hive_biz_db_name = self.entry.cfg_mgr.get('hive-biz', 'database')
        self.hive_cmd = self.entry.cfg_mgr.get("hive-biz", "hive_cmd")
        self.hive_table = None
        self.hive_table_increment = None  # hive 中增量表的名称
        self.table_name_increment = None  # mysql 中增量表的名称
        self.table_name_data_change = MysqlDataChangeTableINfo.TABLE_NAME
        self.debug = self.entry.cfg_mgr.get('debug', False)

    @abc.abstractmethod
    def extract_data(self) -> DataFrame:
        """
        实现业务中具体生成数据表的过程，并返回生成的表的 DataFrame
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def meta_data(self) -> TableMetaData:
        """
        生成的目标表的相关源信息，详细字段见 TableMetaData 类
        :return:
        """
        raise NotImplementedError

    def execute(self):
        try:
            self._check_and_update_table_meta()

            self.logger.info(f'begin exec table {self.table_name}, table info:{self}')

            df = self.extract_data()
            self.logger.info("extract data success")
            table_df = self._save_extract_result(df)
            self.logger.info("save to hdfs success")

            self.load_to_database(table_df)
            self.logger.info("load to database success")

            self.logger.info(f"exec table {self.table_name} success")
        except Exception:
            msg = f'exec table {self.table_name} failed:{traceback.format_exc()}'
            self.logger.error(msg)
            if not self.debug:
                raise

    def load_to_database(self, table_df: DataFrame):

        # 全量数据写入 hive
        table_df.persist()
        exec_shell(f"""{self.hive_cmd} 'alter table {self.hive_table} drop partition (dt="{self.exec_version}")'""")
        table_df.withColumn("dt", fun.lit(self.exec_version))\
            .write.saveAsTable(self.hive_table, format='orc', mode='append', partitionBy='dt')
        self.logger.info("full load to hive success")

        # 全量数据写入 mysql
        self.mysql.jdbc_write(table_df, self.table_name, write_mode='overwrite')
        self.logger.info("full data load to mysql success")

        # 获取增量数据
        increment_df = self._get_increment_df(table_df)
        if (not increment_df) or (not increment_df.take(1)):
            self.logger.info("no increment data")
            return
        table_df.unpersist()
        increment_df.persist()

        # 增量数据写入 hive
        increment_df.withColumn('dt', fun.lit(self.exec_version))\
            .write.format("orc").mode("overwrite").saveAsTable(self.hive_table_increment, partitionBy='dt')
        self.logger.info("increment data load to hive success")

        # 增量数据写入 mysql, mysql 的表是一张统一的增量表, 表结构见 enums
        self._write_increment_data_to_mysql(increment_df)
        increment_df.unpersist()

    def _write_increment_data_to_mysql(self, increment_df: DataFrame):

        def to_data_change_row(row):
            row_dict = row.asDict()
            row_ret = dict()
            for key in table_cols:
                row_ret[key] = row_dict[key]
                row_dict.pop(key)
            row_ret['properties'] = JsonUtils.to_string(row_dict)
            return row_ret

        date_now = DateUtils.now2str()
        table_cols = ['table_name', 'uid', 'timestamp', 'operator']
        data_change_df = increment_df\
            .withColumn('table_name', fun.lit(self.table_name))\
            .withColumn('uid', fun.lit(self.table_meta_data.primary_keys[0]))\
            .withColumn('timestamp', fun.lit(date_now))\
            .rdd\
            .map(to_data_change_row)\
            .toDF()
        self.mysql.jdbc_write(data_change_df, self.table_name_data_change, write_mode='append')
        self.logger.info("increment data load to mysql success")

    def _check_and_update_table_meta(self):
        if not self.table_meta_data:
            raise FriendlyException(f"empty meta data")
        if not self.table_meta_data.destination_table_name:
            raise FriendlyException(f"empty table name")
        self.table_name = self.table_meta_data.destination_table_name
        self.hive_table = f'{self.hive_biz_db_name}.{self.table_name}'

        self.table_name_increment = f'increment_{self.table_name}'
        self.hive_table_increment = f'{self.hive_biz_db_name}.increment_{self.table_name}'

    def _save_extract_result(self, df: DataFrame) -> DataFrame:
        tmp_path = self._get_tmp_path()
        result_path = self._get_result_path()

        # 做一次行动操作，写入临时目录
        df.write.parquet(tmp_path, mode='overwrite')

        # 读取临时目录数据，做写入mysql前的处理，并保存结果到正式结果目录
        df = self.spark.read.parquet(tmp_path)
        columns = self.table_meta_data.destination_table_columns
        df = df.select(*columns) if columns else df  # 读取列报错的不处理，表示数据有问题

        # 因mysql写入时，并发不能太大，这里partition大的需要降低分区数
        if df.rdd.getNumPartitions() > self.max_df_part_to_mysql:
            df = df.repartition(self.max_df_part_to_mysql)
        df.write.parquet(result_path, mode='overwrite')

        return self.spark.read.parquet(result_path)

    def _get_increment_df(self, current_df) -> DataFrame:
        """
        获取增量数据
        通过和前一个版本的数据获得增量数据
        如果没有前一个版本，则直接返回
        有前一个版本的，通过相互获取对方不存在的数据来确认本次变化的数据
        如果不存在变更数据的，直接返回变化数据
        存在数据变更的，在变化数据基础上找出变更数据，将增删改的数据区分后最后再返回
        :param current_df:
        :return:
        """
        latest_dt = HiveUtil.latest_partition_with_date(self.spark, self.hive_table, self.exec_version)
        if not latest_dt:
            self.logger.info("no latest df found")
            return None

        self.logger.info(f"table {self.hive_table} latest dt is: {latest_dt}")
        latest_df = self.spark.sql(f"select * from {self.hive_table} where dt = {latest_dt}").drop('dt')
        add_update_df = current_df.subtract(latest_df)
        del_update_df = latest_df.subtract(current_df)

        if (not self.table_meta_data.has_update_data) or (not self.table_meta_data.primary_keys):
            df_ret = add_update_df.withColumn(TableRowOperation.OPERATION_COLUMN, fun.lit(TableRowOperation.ADD))
            df_del = del_update_df.withColumn(TableRowOperation.OPERATION_COLUMN, fun.lit(TableRowOperation.DELETE))
            return df_ret.unionAll(df_del)

        update_df = add_update_df.alias("L").join(del_update_df, self.table_meta_data.primary_keys).select('L.*')
        add_df = add_update_df.subtract(update_df)
        del_df = del_update_df.join(update_df, self.table_meta_data.primary_keys, how='left_anti')

        ret_df = add_df.withColumn(TableRowOperation.OPERATION_COLUMN, fun.lit(TableRowOperation.ADD))
        update_df = update_df.withColumn(TableRowOperation.OPERATION_COLUMN, fun.lit(TableRowOperation.UPDATE))
        del_df = del_df.withColumn(TableRowOperation.OPERATION_COLUMN, fun.lit(TableRowOperation.DELETE))

        return ret_df.unionAll(update_df).unionAll(del_df)

    def _get_result_path(self) -> str:
        graph_path = self.entry.cfg_mgr.hdfs.get_result_path('graph-biz', 'graph_hdfs_path')
        return os.path.join(graph_path, self.table_name)

    def _get_tmp_path(self) -> str:
        graph_path = self.entry.cfg_mgr.hdfs.get_tmp_path('graph-biz', 'graph_hdfs_path')
        return os.path.join(graph_path, self.table_name)

    def __repr__(self):
        return ','.join([
            f'table_name:{self.table_name}',
            f'dw:{self.hive_dw_db_name}',
            f'biz:{self.hive_biz_db_name}',
            f'version:{self.exec_version}',
            f'meta:{self.table_meta_data}'
        ])

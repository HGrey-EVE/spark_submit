#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: weifuwan@bbdservice.com
:Date: 2021-04-09 16:08
"""
import datetime
import json
import os
import traceback
import re

from pyspark import RDD
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, MapType
from whetstone.core.entry import Entry
# from whetstone.utils.utils_hive import HiveUtils
from pyspark.sql import functions as fun
from yunjian.proj_common.hive_util import HiveUtil
from yunjian.proj_common.common_util import save_text_to_hdfs
from yunjian.proj_common.hbase_util import HbaseWriteHelper
from yunjian.proj_common.date_util import DateUtils


class LogTrack:
    @staticmethod
    def log_track(function):
        def wrapper(*args, **kwargs):
            args[0].logger.info(f" ==== START EXEC method {function.__name__} ====")
            result = function(*args, **kwargs)
            args[0].logger.info(f" ====  ENG  EXEC method {function.__name__} ====")
            return result

        return wrapper


def pre_check(entry: Entry):
    return True

def saveToHdfs(rdd: RDD, path, number=1):
    os.system("hadoop fs -rm -r " + path)
    rdd.repartition(10).saveAsTextFile(path)
## save到hbase
def saveHbase(path: str,entry: Entry):
    current_path = os.path.dirname(os.path.realpath(__file__))
    entry.logger.info("当前路径~~" + current_path)
    log_path = current_path.replace("rose/yunjian","rose/logs")
    os.system(f"""nohup python -u {current_path}/flush_hbase_4.py {path} > {log_path}/flush_hbase_4.log 2>&1 &""")

def transfer(tmp_dict):
    push_data = dict()
    tmp = dict()
    push_data['person_id'] = tmp_dict['person_id']
    tmp[tmp_dict['role']] = tmp_dict['cnt']
    push_data[f"info{tmp_dict['role']}"] = json.dumps(tmp, ensure_ascii=False)
    return push_data

def splicing_data(spark: SparkSession, df: DataFrame, name):
    rdd = df.rdd.map(lambda x: x.asDict()).map(transfer).map(lambda x: (x['person_id'], x[f"info{name}"]))
    schema = StructType([
        StructField("person_id", StringType(), True),
        StructField(f"info{name}", StringType(), True)])
    return spark.createDataFrame(rdd, schema)

def prepare_for_hbase(row: Row):
    tmp = row.asDict(True)
    dic = dict()
    infoshareholder = json.loads(tmp['infoshareholder'])
    infolegal = json.loads(tmp['infolegal'])
    infomanager = json.loads(tmp['infomanager'])
    infocontroller = json.loads(tmp['infocontroller'])

    dic["shareholder"] = infoshareholder['shareholder']
    dic["legal"] = infolegal['legal']
    dic["manager"] = infomanager['manager']
    dic["controller"] = infocontroller['controller']
    person_id = tmp['person_id']

    return person_id + '|' + json.dumps(dic, ensure_ascii=False)

class IndexDesc:
    def __init__(self, role, target_db_name, target_table_name, target_table_field):
        self.role = role
        self.db = target_db_name
        self.table_name = target_table_name
        self.field = target_table_field


class CommonIndexHandler:
    def __init__(self,
                 spark: SparkSession,
                 logger,
                 calc_date_str,
                 basic_output_path,
                 to_hbase_shell_dir,
                 table_name,
                 family_name,
                 source_csv_delimiter,
                 hbase_columns,
                 meta_table_name,
                 meta_row_key
                 ):
        self.spark = spark
        self.logger = logger
        self.calc_date_str = calc_date_str if calc_date_str else datetime.datetime.now().strftime('%Y%m%d')
        self.basic_path = basic_output_path
        self.to_hbase_shell_dir = to_hbase_shell_dir
        self.table_name = table_name
        self.family_name = family_name
        self.source_csv_delimiter = source_csv_delimiter
        self.hbase_columns = hbase_columns
        self.meta_table_name = meta_table_name
        self.meta_row_key = meta_row_key

        self.index_info = []
        self.index_paths = []
    @LogTrack.log_track
    def index_prepare(self, role, target_db_name, target_table_name, target_table_field):
        self.index_info.append(IndexDesc(role, target_db_name, target_table_name, target_table_field))

    @LogTrack.log_track
    def exec_all_index(self):
        for index_desc in self.index_info:
            try:
                self._index_calc(index_desc)
            except Exception:
                self.logger.error(
                    f"exec index: {index_desc.field}, field: {index_desc.field} failed:{traceback.format_exc()}")
        self.logger.info("begin exec merge index")
        shareholder_tmp_path = os.path.join(self.basic_path, 'tmp', 'shareholder', self.calc_date_str)
        df = self.spark.read.parquet(shareholder_tmp_path)
        for i in ['legal','manager','controller']:
            tmp_path = os.path.join(self.basic_path, 'tmp', i, self.calc_date_str)
            in_df = self.spark.read.parquet(tmp_path)
            df = df.join(in_df,'person_id','full')
        index_path = os.path.join(self.basic_path, 'index_4', self.calc_date_str)
        self._calc_and_save_index(df, index_path)
        self.index_paths.append(index_path)
        self._load_to_hbase()

    @LogTrack.log_track
    def _index_calc(self, index_desc: IndexDesc):
        max_dt = HiveUtil.newest_partition(self.spark, f"{index_desc.db}.{index_desc.table_name}")
        self.logger.info(f"index {index_desc.table_name} max dt is:{max_dt}")

        tmp_path = os.path.join(self.basic_path, 'tmp', index_desc.role, self.calc_date_str)
        self.logger.info(f"index {index_desc.field} tmp_path is {tmp_path}")

        sql = self._build_temp_table_sql(index_desc, max_dt)

        role = splicing_data(self.spark, self.spark.sql(sql), index_desc.role)
        role.repartition(100).write.parquet(tmp_path,mode="overwrite")

    @LogTrack.log_track
    def _load_to_hbase(self):
        table_name = f'{self.table_name}{self.calc_date_str}'
        hfile_absolute_dir = os.path.join(self.basic_path, 'tmp', 'index_4_hfile')

        for index_path in self.index_paths:
            hbase_write_helper = HbaseWriteHelper(
                self.logger,
                table_name,
                self.family_name,
                shell_path=os.path.join(self.to_hbase_shell_dir, f"{table_name}_create.shell"),
                hfile_absolute_path=hfile_absolute_dir,
                source_data_absolute_path=index_path,
                source_csv_delimiter=self.source_csv_delimiter,
                hbase_columns=self.hbase_columns,
                meta_table_name=self.meta_table_name,
                meta_row=self.meta_row_key,
                meta_shell_path=os.path.join(self.to_hbase_shell_dir, f"{table_name}_meta_create.shell")
            )
            try:
                hbase_write_helper.exec_write()
            except Exception:
                self.logger.error(
                    f"load table {table_name} path{index_path} to hbase failed:{traceback.format_exc()}")

    @LogTrack.log_track
    def _build_temp_table_sql(self, index_desc: IndexDesc, max_dt):
        condition = ''
        if index_desc.table_name == 'ratio_path_company':
            condition = f" and shareholder_type = 1 and percent >= 0.05"

        sql = f"""
            select
                {index_desc.field} as person_id,
                count(distinct bbd_qyxx_id) cnt,
                '{index_desc.role}' as role
            from (
                select
                    *
                from {index_desc.db}.{index_desc.table_name}
                where dt = {max_dt} {condition}
            ) a
            group by {index_desc.field}
        """
        self.logger.info(f'table {index_desc.table_name} temp sql is:{sql}')
        return sql

    @LogTrack.log_track
    def _calc_and_save_index(self, df, index_path):
        _rdd = df.fillna("{\"shareholder\": 0}", subset=['infoshareholder']) \
            .fillna("{\"legal\": 0}", subset=['infolegal']) \
            .fillna("{\"manager\": 0}", subset=['infomanager']) \
            .fillna("{\"controller\": 0}", subset=['infocontroller']) \
            .filter("person_id != 'null' and person_id != '' and person_id != 'NULL' and person_id is not null") \
            .rdd \
            .map(prepare_for_hbase)
        save_text_to_hdfs(_rdd, index_path, repartition_number=10, force_repartition=True)

def execute(index_handler: CommonIndexHandler, target_db_name):
    index_handler.index_prepare("shareholder",target_db_name, "qyxx_gdxx", "inv_group_id")
    index_handler.index_prepare("legal",target_db_name, "qyxx_basic", "frname_group_id")
    index_handler.index_prepare("manager",target_db_name, "qyxx_baxx", "name_group_id")
    index_handler.index_prepare("controller",target_db_name, "ratio_path_company", "shareholder_cid")

    index_handler.exec_all_index()

def main(entry: Entry):
    spark: SparkSession = entry.spark
    db = entry.cfg_mgr.get("hive", "database")
    basic_output_path = entry.cfg_mgr.get("hdfs", "hdfs_output_path")

    calc_date_str = DateUtils.now2str(fmt='%Y%m%d')

    table_name = entry.cfg_mgr.get("hbase", "table_name")
    family_name = entry.cfg_mgr.get("hbase", "family_name")
    source_csv_delimiter = entry.cfg_mgr.get("hbase", "source_csv_delimiter")
    hbase_columns = entry.cfg_mgr.get("hbase", "hbase_columns")
    meta_table_name = entry.cfg_mgr.get("hbase", "meta_table_name")
    meta_row_key = entry.cfg_mgr.get("hbase", "meta_row_key")

    to_hbase_shell_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'bulkLoad')
    index_handler = CommonIndexHandler(
        spark, entry.logger, calc_date_str, basic_output_path, to_hbase_shell_dir,
        table_name, family_name, source_csv_delimiter, hbase_columns, meta_table_name, meta_row_key)
    execute(index_handler, db)

def post_check(entry: Entry):
    return True

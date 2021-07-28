#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: weifuwan@bbdservice.com
:Date: 2021-02-03 16:08
"""
import datetime
import json
import os
import traceback

from pyspark import RDD
from pyspark.sql.types import StructType, StructField, StringType, MapType, DoubleType
from whetstone.core.entry import Entry
from yunjian.proj_common.hive_util import HiveUtil
from yunjian.proj_common.json_util import JsonUtils
from pyspark.sql import functions as fun, DataFrame, SparkSession
from pyspark.sql import Row, functions as F
from yunjian.proj_common.date_util import DateUtils

from yunjian.proj_common.hbase_util import HbaseWriteHelper


class CnUtils:

    """
    输出路径
    """
    @classmethod
    def get_final_path(cls, entry):
        return "/user/cqxdcyfzyjy/final"
        # return entry.cfg_mgr.get("hdfs", "hdfs_root_path") + entry.cfg_mgr.get("hdfs", "hdfs_final_path")
    """
    临时路径
    """
    @classmethod
    def get_temp_path(cls, entry):
        return entry.cfg_mgr.get("hdfs", "hdfs_root_path") + entry.cfg_mgr.get("hdfs", "hdfs_temp_path")
    """
    获取当月第一天
    """
    @classmethod
    def get_first_day_of_month(cls):
        return DateUtils.now2str(fmt='%Y%m') + '01'
    """
    获取当前时间
    """
    @classmethod
    def now_time(cls):
        return datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    """
    保存到hdfs
    """
    @classmethod
    def saveToHdfs(cls, rdd, path, number = 20):
        os.system("hadoop fs -rm -r " + path)
        rdd.repartition(number).saveAsTextFile(path)
    """
    获取上月月初
    """
    @staticmethod
    def get_first_day_of_last_month():
        return DateUtils.add_date(datetime.datetime.today().replace(day=1),months = -1).strftime('%Y-%m-%d')
    """
    获取上月月末
    """
    @staticmethod
    def get_last_day_of_last_month():
        next_month = DateUtils.add_date(datetime.datetime.today(), months = -1).replace(day=28) + datetime.timedelta(days=4)
        return (next_month - datetime.timedelta(days = next_month.day)).strftime('%Y-%m-%d')
    """
    check data
    """
    @classmethod
    def save_parquet(cls, df: DataFrame,path: str, num_partitions = 20):
        df.repartition(num_partitions).write.parquet(path, mode = "overwrite")

    """
    处理为null
    """
    @classmethod
    def not_null(cls):
        return "nvl(bbd_qyxx_id,'') != '' and bbd_qyxx_id != 'null' and bbd_qyxx_id != 'NULL' and bbd_qyxx_id is not null"
    """
    获取配置对象
    """
    @classmethod
    def get_config_info(cls, entry):
        table_name = entry.cfg_mgr.get("hbase", "table_name")
        family_name = entry.cfg_mgr.get("hbase", "family_name")
        source_csv_delimiter = entry.cfg_mgr.get("hbase", "source_csv_delimiter")
        hbase_columns = entry.cfg_mgr.get("hbase", "hbase_columns")
        meta_table_name = entry.cfg_mgr.get("hbase", "meta_table_name")
        meta_row_key = entry.cfg_mgr.get("hbase", "meta_row_key")
        basic_output_path = entry.cfg_mgr.get("hdfs", "hdfs_output_path")
        db = entry.cfg_mgr.get("hive", "database")
        return Configure(table_name,
                 family_name,
                 source_csv_delimiter,
                 hbase_columns,
                 meta_table_name,
                 meta_row_key,
                 basic_output_path,
                 db)

class Configure:
    def __init__(self,
                 table_name,
                 family_name,
                 source_csv_delimiter,
                 hbase_columns,
                 meta_table_name,
                 meta_row_key,
                 basic_output_path,
                 db):
        self.basic_path = basic_output_path
        self.table_name = table_name
        self.family_name = family_name
        self.source_csv_delimiter = source_csv_delimiter
        self.hbase_columns = hbase_columns
        self.meta_table_name = meta_table_name
        self.meta_row_key = meta_row_key
        self.db = db

    def __str__(self):
        return f"""
             basic_path: {self.basic_path},
             table_name: {self.table_name},
             family_name:{self.family_name},
             source_csv_delimiter: {self.source_csv_delimiter},
             hbase_columns: {self.hbase_columns},
             meta_table_name: {self.meta_table_name},
             meta_row_key: {self.meta_row_key},
             db: {self.db}
        """

class LogTrack:
    @staticmethod
    def log_track(function):
        def wrapper(*args, **kwargs):
            args[1].logger.info(f" ==== START EXEC method {function.__name__} ====")
            result = function(*args, **kwargs)
            args[1].logger.info(f" ====  ENG  EXEC method {function.__name__} ====")
            return result

        return wrapper

class CalIndex:
    index_paths = []

    @classmethod
    def append_path(cls, entry, path):
        entry.logger.info(f"==== {path} ====")
        cls.index_paths.append(path)

    @staticmethod
    @LogTrack.log_track
    def exec_sql(sql: str, entry: Entry):
        entry.logger.info(sql)
        return entry.spark.sql(sql)

    @classmethod
    def common_index(cls,
                     entry: Entry,
                     table_name: str,
                     field: str,
                     db: str,
                     date_field: str,
                     index_name: str):
        max_dt = HiveUtil.newest_partition(entry.spark, f"{db}.{table_name}")

        column = ""
        condition = ""
        if date_field:
            column = f"bbd_qyxx_id,{date_field},{field}"
            condition = f", cast(substr({date_field},0,4) as string) as {date_field}"
        else:
            column = f"bbd_qyxx_id,{field}"

        if index_name == 'employment':
            condition = ",employ_num"

        if field == 'start_date':
            column = f"bbd_qyxx_id,{field}"
            condition = f", cast(regexp_replace(substr(start_date,0,7), '-', '') as string) as {date_field},employ_num"
        _sql = ""

        if field != 'start_date':
            _sql = f"""
                        select
                            bbd_qyxx_id, 
                            {field} 
                            {condition}
                        from {db}.{table_name}
                        where dt = '{max_dt}' and {CnUtils.not_null()}
                    """
        else:
            _sql = f"""
                        select
                            bbd_qyxx_id, 
                            cast(regexp_replace(substr(start_date,0,7), '-', '') as string) as start_date,
                            employ_num
                        from {db}.{table_name}
                        where dt = '{max_dt}' and {CnUtils.not_null()}
                    """
        common_df: DataFrame = None

        if index_name == 'employment':
            common_df = cls.exec_sql(_sql, entry).fillna(0,subset=['employ_num']).groupBy(column.split(",")).agg(
                F.sum("employ_num").alias(f"{table_name}_{field}_sum")
            ).fillna(0,[f'{table_name}_{field}_sum']).fillna("null")
        else:
            common_df = cls.exec_sql(_sql, entry).groupBy(column.split(",")).agg(
                F.countDistinct("bbd_qyxx_id").alias(f"{table_name}_{field}_cnt")
            ).fillna(0,[f'{table_name}_{field}_cnt']).fillna("null")

        if date_field:
            cls.df_to_json(entry, common_df, index_name, field, date_field)
        else:
            flag = cls.df_to_json_before(entry, common_df, index_name, field, date_field)
            if flag == 1:
                entry.logger.info("------ 添加befor的path -------")
                cls.append_path(entry,CnUtils.get_final_path(entry) + f"/tmp/req_3_1/{index_name}/{field}")
        # return

    @classmethod
    def common_index_out(cls,
                     entry: Entry,
                     table_name: str,
                     field: str,
                     db: str,
                     date_field: str,
                     index_name: str):
        gd_max_dt = HiveUtil.newest_partition(entry.spark, f"{db}.{table_name}")
        ba_max_dt = HiveUtil.newest_partition(entry.spark, f"{db}.qyxx_basic")

        _sql = f"""
            select
               R.bbd_qyxx_id,
               R.company_industry as industry,
               R.company_province as province,
               cast(regexp_replace(substr(R.esdate,0,7), '-', '') as string) as esdate,
               case when L.sub_amount is null then 0 when L.sub_amount = 'None' then 0 when nvl(L.sub_amount,'') = '' then 0 when L.sub_amount = 'null' then 0 else L.sub_amount end sub_amount
            from (
               select
                  *
               from {db}.{table_name} where {CnUtils.not_null()} and dt = '{gd_max_dt}') L
               inner join 
               (
               select 
                  * 
               from {db}.qyxx_basic where {CnUtils.not_null()} and dt = '{ba_max_dt}') R 
            on L.bbd_qyxx_id = R.bbd_qyxx_id
        """
        column = f"bbd_qyxx_id,province,{field}"
        ## 处理amount为null的情况，因为fillna不起作用
        def map1(amount):
            result = amount
            if amount == 'null' and amount == 'None' and amount == '':
                result = 0.0
            return result
        udf = fun.udf(map1, DoubleType())
        _rdd = cls.exec_sql(_sql, entry).groupBy(column.split(",")).agg(
                F.countDistinct("bbd_qyxx_id").alias("cnt"),
                F.sum("sub_amount").alias("amount")
            ).fillna(0,subset=['cnt','amount']).fillna("null").rdd \
            .map(lambda x: cls.transfer(x, index_name, field)) \
            .groupByKey() \
            .map(lambda x: (x[0],list(x[1]))) \
            .map(cls.transfer2) \
            .groupByKey() \
            .map(lambda x: (x[0],list(x[1]))).map(cls.trans_v2)\
            .map(lambda x: x[0] + "|" + json.dumps(x[1], ensure_ascii=False))
        CnUtils.saveToHdfs(_rdd,CnUtils.get_final_path(entry) + f"/tmp/req_3_7/{index_name}/{field}")
        cls.append_path(entry,CnUtils.get_final_path(entry) + f"/tmp/req_3_7/{index_name}/{field}")

    @staticmethod
    def transfer(row, index_name, field):
        tmp_data = row.asDict(True)
        push_data = dict()
        bbd_qyxx_id = tmp_data['bbd_qyxx_id'] + "_" + index_name + "_" + field
        province = tmp_data['province']
        push_data[field] = tmp_data[field]
        push_data['cnt'] = tmp_data['cnt']
        push_data['amount'] = tmp_data['amount']
        key = bbd_qyxx_id + "``" + province
        return (key, push_data)

    @staticmethod
    def transfer2(x):
        bbd_qyxx_id = x[0].split("``")[0]
        province = x[0].split("``")[1]
        value = x[1]
        result_data = dict()
        result_data[province] = value
        return (bbd_qyxx_id, result_data)


    @staticmethod
    def json_tran(row: Row, index_name: str, field: str, date_field: str):
        tmp_data = row.asDict(True)
        push_data = dict()
        row_key = ""
        if date_field:
            row_key = tmp_data['bbd_qyxx_id'] + '^' + index_name + '^' + field + '^' + str(tmp_data[date_field])
        else:
            row_key = tmp_data['bbd_qyxx_id'] + '^' + index_name + '^' + field
        push_data["bbd_qyxx_id"] = row_key
        del tmp_data['bbd_qyxx_id']
        field_data = tmp_data[field]
        del tmp_data[field]
        if date_field:
            del tmp_data[date_field]
        push_data["info"] = {field_data: str(v) for k, v in tmp_data.items()}
        return push_data

    @staticmethod
    def tran(row: Row):
        tmp_data = row.asDict(True)
        push_data = dict()
        push_data["data"] = {k: json.dumps(v, ensure_ascii=False) if k in ['info'] else v for k, v in tmp_data.items()}
        return push_data

    @staticmethod
    def prepare_for_hbase(row: str):
        tmp_data = json.loads(row)
        info2 = tmp_data['data']['info']
        bbd_qyxx_id = tmp_data['data']['bbd_qyxx_id']
        sj = json.loads(info2)
        dic = dict()
        for i in sj:
            for m in i:
                dic[m] = i[m]
        # return bbd_qyxx_id.replace("^","_") + "``" + json.dumps(dic, ensure_ascii=False)
        return bbd_qyxx_id + "``" + json.dumps(dic, ensure_ascii=False)

    @staticmethod
    def prepare_for_hbase_h(row: str):
        tmp_data = json.loads(row)
        info2 = tmp_data['data']['info']
        bbd_qyxx_id = tmp_data['data']['bbd_qyxx_id']
        sj = json.loads(info2)
        dic = dict()
        for i in sj:
            for m in i:
                dic[m] = i[m]
        return bbd_qyxx_id.replace("^", "_") + "|" + json.dumps(dic, ensure_ascii=False)
        # return bbd_qyxx_id + "|" + json.dumps(dic, ensure_ascii=False)

    @staticmethod
    def json_tran_v2(row: Row):
        tmp_data = row.asDict(True)
        push_data = dict()
        push_data["bbd_qyxx_id"] = tmp_data['bbd_qyxx_id']
        del tmp_data['bbd_qyxx_id']
        date_data = tmp_data["date"]
        del tmp_data["date"]
        try:
            push_data["info"] = {date_data: json.loads(str(v)) for k, v in tmp_data.items()}
        except Exception:
            print("====", tmp_data)
            raise
        return push_data
    @staticmethod
    def trans_v2(tup):
        id = tup[0]
        di = dict()
        for i in tup[1]:
            for k, v in i.items():
                di[k] = v
        return (id, di)

    @classmethod
    def df_to_json_before(cls, entry, index: DataFrame, index_name, field, date_field):
        if index.rdd.isEmpty():
            entry.logger.info(f"rdd -- index_name-{index_name}-field-{field}-为null")
            return 0
        rdd_res = index.rdd \
            .map(lambda x: cls.json_tran(x, index_name, field, date_field)) \
            .map(lambda x: (x['bbd_qyxx_id'], x['info'])) \
            .groupByKey() \
            .map(lambda m: (m[0], list(m[1]))) \
            .toDF(["bbd_qyxx_id", "info"]) \
            .rdd.map(cls.tran)

        schema = StructType([StructField("data", MapType(StringType(), StringType()), True)])

        # 数据保存路径
        entry.spark.createDataFrame(rdd_res, schema) \
            .write \
            .json(CnUtils.get_final_path(entry) + f"/tmp/req_3/{index_name}/{field}", mode="overwrite")

        _rdd = entry.spark.sparkContext.textFile(CnUtils.get_final_path(entry) + f"/tmp/req_3/{index_name}/{field}").map(cls.prepare_for_hbase_h)
        CnUtils.saveToHdfs(_rdd,CnUtils.get_final_path(entry) + f"/tmp/req_3_1/{index_name}/{field}")
        _rdd2 = entry.spark.sparkContext.textFile(CnUtils.get_final_path(entry) + f"/tmp/req_3/{index_name}/{field}").map(cls.prepare_for_hbase)
        CnUtils.saveToHdfs(_rdd2, CnUtils.get_final_path(entry) + f"/tmp/req_3_1b/{index_name}/{field}")
        return 1

    @classmethod
    def df_to_json(cls, entry, index: DataFrame, index_name, field, date_field):

        flag = cls.df_to_json_before(entry,index,index_name,field,date_field)
        if flag == 0:
            entry.logger.info("----- rdd 为null 不进行下面的计算 ------")
            return
        df_v2 = entry.spark.sparkContext \
            .textFile(CnUtils.get_final_path(entry) + f"/tmp/req_3_1b/{index_name}/{field}") \
            .map(lambda p: (
                p.split("``")[0].split("^")[0] + "_" + p.split("``")[0].split("^")[1] + "_" + p.split("``")[0].split("^")[2],
                p.split("``")[0].split("^")[3], p.split("``")[1])) \
            .toDF(["bbd_qyxx_id", "date", "index_info"])

        rdd_v2 = df_v2.rdd.map(cls.json_tran_v2).map(lambda x: (x['bbd_qyxx_id'], x['info'])).groupByKey().map(
            lambda m: (m[0], list(m[1]))).map(cls.trans_v2).map(lambda x: x[0].replace("^","_") + "|" + json.dumps(x[1], ensure_ascii=False))
        CnUtils.saveToHdfs(rdd_v2,CnUtils.get_final_path(entry) + f"/tmp/req_3_2/{index_name}/{field}")
        cls.append_path(entry,CnUtils.get_final_path(entry) + f"/tmp/req_3_2/{index_name}/{field}")

    @classmethod
    @LogTrack.log_track
    def _load_to_hbase(cls,
                       entry,config: Configure):
        calc_date_str = DateUtils.now2str(fmt='%Y%m%d')
        table_name = f'{config.table_name}{calc_date_str}'
        hfile_absolute_dir = os.path.join(config.basic_path, 'tmp', 'index_3_hfile')
        entry.logger.info(f"-------path-------{cls.index_paths}-------path-----")
        for index_path in cls.index_paths:
            entry.logger.info(f"-------working-------{index_path}-------working-----")
            hbase_write_helper = HbaseWriteHelper(
                entry.logger,
                table_name,
                config.family_name,
                shell_path="",
                hfile_absolute_path=hfile_absolute_dir,
                source_data_absolute_path=index_path,
                source_csv_delimiter=config.source_csv_delimiter,
                hbase_columns=config.hbase_columns,
                meta_table_name=config.meta_table_name,
                meta_row=config.meta_row_key,
                meta_shell_path=""
            )
            try:
                hbase_write_helper.exec_write()
            except Exception:
                entry.logger.error(
                    f"load table {table_name} to hbase failed:{traceback.format_exc()}")

    @classmethod
    @LogTrack.log_track
    def merge(cls, entry, db):
        # 开庭公告
        # for field in ["action_cause","province", "trial_court", "accuser", "defendant"]:
        #     cls.common_index(entry, "ktgg", field, db, "register_date","ktgg")
        for field in ["action_cause","trial_court"]:
            cls.common_index(entry, "ktgg", field, db, "register_date","ktgg")

        # 裁判文书
        # for field in ["action_cause","province", "trial_court", "accuser", "defendant", "doc_type", "case_type"]:
        #     cls.common_index(entry,"legal_adjudicative_documents",field, db, "sentence_date","zgcpwsw")
        # # # 被执行人
        # for field in ["province","exec_court_name"]:
        #     cls.common_index(entry, "legal_persons_subject_to_enforcement", field, db, "register_date", "zhixing")
        # # # 失信被执行人
        # for field in ["province","exec_court_name"]:
        #     cls.common_index(entry,"legal_dishonest_persons_subject_to_enforcement",field, db, "register_date","dishonesty")
        # # # 立案信息
        # for field in ["case_reason","area"]:
        #     cls.common_index(entry,"company_court_register",field, db, "","laxx")
        # # # 投资事件
        # for field in ["round", "category"]:
        #     cls.common_index(entry, "organization_invest", field, db, "", "invest")
        # # # 招聘信息
        # for field in ["job_first_class", "start_date"]:
        #     cls.common_index(entry, "company_employment", field, db, "", "employment")
        # # # 对外投资
        # for field in ["industry","esdate"]:
        #     cls.common_index_out(entry, "qyxx_gdxx", field, db, "", "out_invest")

        # entry.spark.stop()
        #
    @classmethod
    def index_and_habse(cls, entry, config: Configure):
        try:
            cls.merge(entry, config.db)
        except Exception:
            entry.logger.error(
                f"exec index failed:{traceback.format_exc()}")
        # cls._load_to_hbase(entry, config)

def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    ## 获取配置信息
    config = CnUtils.get_config_info(entry)
    entry.logger.info("配置信息如下:")
    entry.logger.info(config)
    ## 计算指标并写到hbase中
    CalIndex.index_and_habse(entry,config)


def post_check(entry: Entry):
    return True

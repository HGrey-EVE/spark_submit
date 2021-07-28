#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 产业分布
:Author: weifuwan@bbdservice.com
:Date: 2021-04-27 16:08
"""
import datetime
import os

from pyspark.sql.types import IntegerType
from whetstone.core.entry import Entry
from cqxdcy.proj_common.hive_util import HiveUtil
from cqxdcy.proj_common.json_util import JsonUtils
from pyspark.sql import functions as fun, DataFrame
from pyspark.sql import Row, functions as F
from cqxdcy.proj_common.date_util import DateUtils
from cqxdcy.proj_common.log_track_util import LogTrack as T


class CnUtils:
    """
    原始路径
    """
    @classmethod
    def get_source_path(cls, entry):
        return entry.cfg_mgr.hdfs.get_input_path("hdfs", "hdfs_path")
    """
    输出路径
    """
    @classmethod
    def get_final_path(cls, entry):
        return entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_path")
    """
    临时路径
    """
    @classmethod
    def get_temp_path(cls, entry):
        return entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path")
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
    存parquet文件
    """
    @classmethod
    def save_parquet(cls, df: DataFrame,path: str, num_partitions = 20):
        df.repartition(num_partitions).write.parquet(path, mode = "overwrite")

    """
    存csv文件
    """
    @classmethod
    def save_csv(cls, df: DataFrame, path: str, num_partitions = 1):
       df.repartition(1).write.csv(path,mode='overwrite', header=True, sep="\t")


class CalIndex:

    @staticmethod
    @T.log_track
    def exec_sql(sql: str, entry: Entry):
        entry.logger.info(sql)
        return entry.spark.sql(sql)

    @staticmethod
    def get_company_county(entry: Entry):
        """
        获取省市区
        :return: DataFrame
        """
        dt = HiveUtil.newest_partition(entry.spark, 'dw.company_county')
        entry.spark.sql("""
                      select company_county, province, city, region
                      from 
                          (select code as company_county, province, city, district as region, 
                                      (row_number() over(partition by code order by tag )) as row_num
                          from dw.company_county 
                          where dt = '{dt}' and tag != -1) b 
                      where row_num = 1
                 """.format(dt=dt)).createOrReplaceTempView('company_country')

    @staticmethod
    def cal_qyxx_basic(entry: Entry):
        entry.spark.read.parquet(f"{CnUtils.get_source_path(entry)}/industry_divide").createOrReplaceTempView("qyxx_basic")

    @classmethod
    @T.log_track
    def cal_province_city_area(cls, entry: Entry):
        cls.cal_qyxx_basic(entry)
        cls.get_company_county(entry)
        _sql = """
                select 
                    R.*,
                    L.province,
                    L.city,
                    L.region as area
                from company_country L 
                right join qyxx_basic R 
                on L.company_county = R.company_county
            """
        return cls.exec_sql(_sql, entry)

    @classmethod
    def index_pool(cls, entry):
        _sql = """
        select
             L.bbd_qyxx_id,
             qy_sxbzxr_num as 483C,
             qy_cpws_num as 467C,
             qy_zzzb as `1Y`,
             qy_fddbrbg_num_6month as 382C,
             qy_gqdj_num as 110C,
             qy_xzcf_num as 475C,
             qy_qs_num as 59B,
             qy_jyyc_num as 132C,
             qy_fddbrbg_num_2year as 383C,
             qy_ktgg_num as 133C,
             qy_zpfb_num as 489C,
             qy_zl_num as 449C,
             qy_sb_num as 161C,
             qy_zhaob_num as 294C,
             qy_zhongb_num as 295C,
             qy_ymba_is as `17Y`
        from dw.index_pool L
        inner join qyxx_basic R
        on L.bbd_qyxx_id = R.bbd_qyxx_id
        """
        CnUtils.save_parquet(cls.exec_sql(_sql, entry), f"{CnUtils.get_temp_path(entry)}/risk/index_pool")

    @classmethod
    def index_develop(cls,entry ,contact_way:str, tmp_table:str, tmp_table2:str, name:str):

        _sql_1 = f"""
        select
         L.bbd_qyxx_id,
         L.contact_way,
         L.contact_info
        from (
           SELECT
            bbd_qyxx_id,
            contact_way,
            contact_info
           FROM dw.contact_info
           WHERE dt = '{HiveUtil.newest_partition(entry.spark, 'dw.contact_info')}' and nvl(bbd_qyxx_id,'') != ''
           and contact_way = '{contact_way}'
        ) L
        inner join qyxx_basic R
        on L.bbd_qyxx_id = R.bbd_qyxx_id
        """
        cls.exec_sql(_sql_1, entry).dropDuplicates(["bbd_qyxx_id"]).createOrReplaceTempView(tmp_table)

        _sql_2 = f"""
        SELECT
         contact_info,
         count(1) as contact_num
        FROM {tmp_table}
        group by contact_info
        """
        cls.exec_sql(_sql_2, entry).createOrReplaceTempView(tmp_table2)

        _sql_3 = f"""
        SELECT
         L.bbd_qyxx_id,
         R.contact_num as {name}
        FROM {tmp_table} L
        inner join {tmp_table2} R
        on L.contact_info = R.contact_info
        """
        CnUtils.save_parquet(cls.exec_sql(_sql_3, entry), f"{CnUtils.get_temp_path(entry)}/risk/{name}")


    @classmethod
    def cal_index(cls, entry: Entry):
        cls.cal_qyxx_basic(entry)
        cls.index_pool(entry)
        cls.index_develop(entry,"phone","tmp_phone","tmp_phone2","F1")
        cls.index_develop(entry,"email","tmp_email","tmp_email2","F2")
        cls.index_develop(entry,"address","tmp_address","tmp_address2","F3")

    @classmethod
    def merge_index(cls, entry: Entry):

        index_pool = entry.spark.read.parquet(f"{CnUtils.get_temp_path(entry)}/risk/index_pool")
        F1 = entry.spark.read.parquet(f"{CnUtils.get_temp_path(entry)}/risk/F1")
        F2 = entry.spark.read.parquet(f"{CnUtils.get_temp_path(entry)}/risk/F2")
        F3 = entry.spark.read.parquet(f"{CnUtils.get_temp_path(entry)}/risk/F3")

        result: DataFrame = entry.spark.sql("select * from qyxx_basic")

        for i in [index_pool, F1, F2, F3]:
            result = result.join(i,"bbd_qyxx_id","left")
        result.createOrReplaceTempView("res_tmp")
        cls.cal_province_city_area(entry).createOrReplaceTempView("qy_company")
        _sql = f"""
        select
         R.bbd_qyxx_id,
         R.company_name,
         R.province,
         R.city,
         R.area as district,
         R.industry,
         R.sub_industry,
         483C,467C,`1Y`,382C,110C,475C,59B,132C,383C,133C,489C,449C,161C,294C,295C,`17Y`,F1,F2,F3
        from res_tmp L
        left join qy_company R
        on L.bbd_qyxx_id = R.bbd_qyxx_id
        """
        CnUtils.save_csv(cls.exec_sql(_sql, entry), f"{CnUtils.get_temp_path(entry)}/cq_xdcy_invest_risk_index")

def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    ## 入口
    CalIndex.cal_index(entry)
    CalIndex.cal_qyxx_basic(entry)
    CalIndex.merge_index(entry)

def post_check(entry: Entry):
    return True
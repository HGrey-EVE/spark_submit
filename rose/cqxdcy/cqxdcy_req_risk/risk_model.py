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
    获取当月第一天
    """
    @classmethod
    def get_first_day_of_month_v2(cls):
        return DateUtils.now2str(fmt='%Y-%m') + '-01'
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
    def save_to_hdfs(cls, rdd, path, number = 20):
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


    @classmethod
    def handle_model_data(cls, entry: Entry):
        ## 获取模型数据
        risk_scores_df = entry.spark.read.csv(f"{CnUtils.get_temp_path(entry)}/model/risk_result.csv",
                                        sep=',', header=True)
        ## 获取指标数据
        fx_mx_index_df = entry.spark.read.csv(f"{CnUtils.get_temp_path(entry)}/cq_xdcy_invest_risk_index", sep='\t',
                                        header=True)

        def to_json_data(row: Row) -> Row:
            tmp_data = row.asDict(True)

            return {k: f"{v}^{k}" for k, v in tmp_data.items()}

        risk_scores_rdd = risk_scores_df.rdd.map(to_json_data)
        fx_mx_index_rdd = fx_mx_index_df.rdd.map(to_json_data)

        CnUtils.save_to_hdfs(risk_scores_rdd, CnUtils.get_temp_path(entry) + "/risk_scores_rdd_02")
        CnUtils.save_to_hdfs(fx_mx_index_rdd, CnUtils.get_temp_path(entry) + "/fx_mx_index_rdd_02")
        entry.spark.read.json(CnUtils.get_temp_path(entry) + "/risk_scores_rdd_02").createOrReplaceTempView("risk_scores_tb")
        entry.spark.read.json(CnUtils.get_temp_path(entry) + "/fx_mx_index_rdd_02").createOrReplaceTempView("fx_mx_index_tb")

        entry.spark.sql(f"""select bbd_qyxx_id ,
                      concat_ws(",",483C_score,467C_score,1Y_score,382C_score,110C_score,475C_score,59B_score,132C_score,383C_score,133C_score,489C_score,449C_score,161C_score,294C_score,295C_score,17Y_score,F1_score,F2_score,F3_score) scroe
                      from risk_scores_tb""").cache().createOrReplaceTempView('risk_scores_json_tmp')

        entry.spark.sql(f"""select bbd_qyxx_id ,
                        concat_ws(",",483C,467C,`1Y`,382C,110C,475C,59B,132C,383C,133C,489C,449C,161C,294C,295C,`17Y`,F1,F2,F3) scroe
                      from fx_mx_index_tb""").cache().createOrReplaceTempView('fx_mx_index_rdd_tmp')

        entry.spark.sql("""
                select
                    split(bbd_qyxx_id,"\\\^")[0] bbd_qyxx_id,
                    itemp_score,
                    score,
                    substr(itemp_name,0,length(itemp_name)-6) itemp_name
                from (
                    select bbd_qyxx_id,item_score as score,split(item_score,"\\\^")[0] itemp_score,split(item_score,"\\\^")[1] itemp_name from risk_scores_json_tmp 
            lateral view explode(split(scroe,',')) num as item_score
                ) a
            """).createOrReplaceTempView('risk_scores_json_res')

        entry.spark.sql("""
                select
                    split(bbd_qyxx_id,"\\\^")[0] bbd_qyxx_id,
                    itemp_score as itemp_score_index,
                    score,
                    itemp_name
                from (
                    select bbd_qyxx_id,item_score as score, split(item_score,"\\\^")[0] itemp_score,split(item_score,"\\\^")[1] itemp_name from fx_mx_index_rdd_tmp 
            lateral view explode(split(scroe,',')) num as item_score
                ) a
            """).createOrReplaceTempView('fx_mx_index_json_res')

        entry.spark.sql(
            "select R.*,L.itemp_score_index from fx_mx_index_json_res L inner join risk_scores_json_res R on L.bbd_qyxx_id = R.bbd_qyxx_id and L.itemp_name = R.itemp_name").createOrReplaceTempView('merge')

        ## 名称处理
        entry.spark.sql("""
                select
                    bbd_qyxx_id,
                    item_score,
                    item_name,
                    case when item_name='483C' then '企业失信被执行人数量'
                        when item_name='467C' then '企业裁判文书数量'
                        when item_name='1Y' then '企业注册资本'
                        when item_name='382C' then '企业法定代表人变更数量近六个月'
                        when item_name='110C' then '企业股权冻结数量'
                        when item_name='475C' then '企业行政处罚数量'
                        when item_name='59B' then '企业欠税数量'
                        when item_name='132C' then '企业经营异常数量'
                        when item_name='383C' then '企业法定代表人变更数量近两年'
                        when item_name='133C' then '企业开庭公告数量'
                        when item_name='489C' then '企业招聘发布数量'
                        when item_name='449C' then '企业专利数量'
                        when item_name='161C' then '企业商标数量'
                        when item_name='294C' then '企业招标数量'
                        when item_name='295C' then '企业中标数量'
                        when item_name='17Y' then '企业域名备案与否'
                        when item_name='F1' then '多公司共用同联系电话'
                        when item_name='F2' then '多公司共用同邮箱'
                        when item_name='F3' then '多公司共用同注册地址'
                    else '' end as item_name_n,
                    item_value
                from (
                    select 
                        bbd_qyxx_id,
                        itemp_score as item_score,
                        itemp_name as item_name,
                        itemp_score_index as item_value
                    from merge
                    ) a
            """).createOrReplaceTempView('merge_09')

        ## 风险类型处理
        entry.spark.sql("""
                select
                    bbd_qyxx_id,
                    item_score,
                    item_name_n as item_name_2,
                    item_value,
                    case when item_name rlike 'F1|F2|F3' then '结构风险'
                             when item_name rlike '483C|467C|1Y|382C|110C|475C|59B|132C|383C|133C' then '合规性风险'
                             when item_name rlike '489C|449C|161C|294C|295C|17Y' then '虚假经营风险'
                             else '' end as risk_type
                from merge_09
            """).createOrReplaceTempView('merge_tmp_03')

        df_merge = entry.spark.sql(f"""
                select
                    bbd_qyxx_id,
                    item_score,
                    item_name_2 as item_name,
                    item_value,
                    risk_type,
                    '{entry.version[0:4]}-{entry.version[4:6]}-{entry.version[6:8]}' as statistics_time,
                    '{CnUtils.now_time()}' as update_time
                from merge_tmp_03
            """)

        def to_push_data(row: Row) -> Row:
            tmp_data = row.asDict(True)
            print(tmp_data)
            push_data = {}
            tn = "cq_xdcy_invest_risk"
            push_data["tn"] = tn
            push_data["data"] = tmp_data
            return JsonUtils.to_string(push_data)

        rdd_res_02 = df_merge.rdd.map(to_push_data)
        CnUtils.save_to_hdfs(rdd_res_02, f"{CnUtils.get_final_path(entry)}/cq_xdcy_invest_risk")


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    ## 入口
    CalIndex.handle_model_data(entry)


def post_check(entry: Entry):
    return True

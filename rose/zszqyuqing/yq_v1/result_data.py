# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 

Date: 2021/7/21 10:36
"""

import os
import re
from typing import Text
from datetime import date

# 由于MD5模块在python3中被移除，在python3中使用hashlib模块进行md5操作
from hashlib import md5
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from whetstone.core.entry import Entry
from zszqyuqing.proj_common.email_send import SendEmail

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from pyspark import SparkConf, Row
from pyspark.sql import SparkSession, DataFrame

from zszqyuqing.proj_common.hive_util import HiveUtil

DISPLAY_SQL = True

LIHAO = "企业合作|重大交易|并购重组|企业盈利|产品发布/升级|股权融资|债权融资|债务重组|企业荣誉|人才引进"
LIKONG = "停业破产|安全事故|工程质量|信息泄露|企业亏损|大量裁员|组织架构变动|制假售假|虚假宣传|环境污染|行业处罚|高管负面|财务欺诈|履行连带担保责任|歇业停业|重组失败|业绩下滑|资产负面|资金困难|账户风险|实际控制人变更|评级调整|涉嫌传销诈骗|涉嫌非法集资|信息披露违规|实际控制人涉及诉讼|证券交易违规|产品反馈|企业退市|股权变动|债务抵押|品牌声誉|实际控制人涉诉仲裁"
ZHONGXING = "产品销售|股权投资|债权投资|对外股票减持|对外股票增持"


# 获取数仓表的最新分区日期
def newest_dt(table, entry: Entry, dw="dw"):
    return HiveUtil.newest_partition(entry.spark, table_name=f"{dw}.{table}")


# 执行sql得到DataFrame
def ss_sql(entry: Entry, sql: Text) -> DataFrame:
    if DISPLAY_SQL:
        entry.logger.info(sql)
    return entry.spark.sql(sql)


# 判断公司是否是上市公司
@udf(returnType=StringType())
def get_is_public_company(bbd_qyxx_id, type):
    if bbd_qyxx_id is not None and 'a' in type and 'h' in type:
        return "A+H股上市公司"
    if bbd_qyxx_id is not None and 'a' in type:
        return "A股上市公司"
    elif bbd_qyxx_id is not None and 'h' in type:
        return "港股上市公司"
    else:
        return "非上市公司"


# 得到关联上市公司
def get_related_public_company(entry: Entry, bbd_qyxx_id):
    self_pub_company_num = ss_sql(entry, f"""
        select
            bbd_qyxx_id
        from pj.ms_ipo_company ic
        where dt={newest_dt('ms_ipo_company', entry, 'pj')}
            and ic.bbd_qyxx_id='{bbd_qyxx_id}'
        """).count()
    # 若自身为上市公司，则该数据为空
    if self_pub_company_num > 0:
        return ""

    # 若自身为非上市公司，则该数据为关联度最近的上市公司；
    related_pub_company_name = ss_sql(entry, f"""
        select
            company_name
        from pj.ms_ipo_company_related_party icrp
        where dt={newest_dt('ms_ipo_company_related_party', entry, 'pj')}
            and icrp.associated_id='{bbd_qyxx_id}'
        order by associated_degree
        limit 1
        """).take(1)[0]

    return related_pub_company_name["company_name"]


# 得到关联上市公司关系
def get_related_public_company_relation(entry: Entry, bbd_qyxx_id):
    self_pub_company_num = ss_sql(entry, f"""
        select
            bbd_qyxx_id
        from pj.ms_ipo_company ic
        where dt={newest_dt('ms_ipo_company', entry, 'pj')}
            and ic.bbd_qyxx_id='{bbd_qyxx_id}'
        """).count()
    # 若自身为上市公司，则该数据为空
    if self_pub_company_num > 0:
        return ""

    # 若自身为非上市公司，则该数据为关联度最近的上市公司关系；
    associated_degree = ss_sql(entry, f"""
        select
            associated_degree
        from pj.ms_ipo_company_related_party icrp
        where dt={newest_dt('ms_ipo_company_related_party', entry, 'pj')}
            and icrp.associated_id='{bbd_qyxx_id}'
        order by associated_degree
        limit 1
        """).take(1)[0]

    return associated_degree["associated_degree"]


# 判断事件类型
def get_event_property(event_type):
    if event_type in LIHAO:
        return "利好事件"
    elif event_type in LIKONG:
        return "利空事件"
    elif event_type in ZHONGXING:
        return "中性事件"


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):
    # 只统计主体(subject_id）或客体（object_id）在关联方名录中的数据
    zszq_event_df_sub = ss_sql(entry, f"""
        select
            id,
            event_subject,
            subject_id,
            event_object,
            object_id,
            event_type,
            bbd_url,
            bbd_unique_id,
            search_id,
            event_main
        from pj.ms_ipo_raw_event_main
        where dt={newest_dt('ms_ipo_raw_event_main', entry, 'pj')}
            and subject_id in (
                SELECT
                    associated_id
                FROM pj.ms_ipo_company_related_party_new icrp
                where dt={newest_dt('ms_ipo_company_related_party_new', entry, 'pj')}
            )
        """)
    zszq_event_df_obj = ss_sql(entry, f"""
        select
            id,
            event_subject,
            subject_id,
            event_object,
            object_id,
            event_type,
            bbd_url,
            bbd_unique_id,
            search_id,
            event_main
        from pj.ms_ipo_raw_event_main
        where dt={newest_dt('ms_ipo_raw_event_main', entry, 'pj')}
            and object_id in (
                SELECT
                    associated_id
                FROM pj.ms_ipo_company_related_party_new icrp
                where dt={newest_dt('ms_ipo_company_related_party_new', entry, 'pj')}
            )
        """)
    zszq_event_df = zszq_event_df_sub.union(zszq_event_df_obj)
    zszq_event_df.createOrReplaceTempView("zszq_event_table")

    # 得到上市公司信息
    ica_df = ss_sql(entry, f"""
        select
            bbd_qyxx_id,
            'a' as type
        from pj.ms_ipo_company_a ic
        where dt={newest_dt('ms_ipo_company_a', entry, 'pj')}
        """)
    ich_df = ss_sql(entry, f"""
        select
            bbd_qyxx_id,
            'h' as type
        from pj.ms_ipo_company_h ic
        where dt={newest_dt('ms_ipo_company_h', entry, 'pj')}
        """)
    ica_df.union(ich_df).createOrReplaceTempView("ic_table_old")
    ss_sql(entry, f"""
        select
          bbd_qyxx_id,
          collect_list(type) over(partition by bbd_qyxx_id) type
        from ic_table_old
        """).dropDuplicates().createOrReplaceTempView("ic_table")

    # 补充主体公司类型(是否上市公司)
    subject_df = ss_sql(entry, f"""
        select
          zet.id,
          zet.event_subject,
          zet.subject_id,
          zet.event_object,
          zet.object_id,
          zet.event_type,
          zet.bbd_url,
          zet.bbd_unique_id,
          zet.event_main,
          it.bbd_qyxx_id,
          it.type,
          1 as subject_type
        from zszq_event_table zet
        left join ic_table it
        on zet.subject_id=it.bbd_qyxx_id
        """)
    subject_df = subject_df.withColumn("subject_type", get_is_public_company("bbd_qyxx_id", "type")).select("id",
                                                                                                            "event_subject",
                                                                                                            "subject_id",
                                                                                                            "event_object",
                                                                                                            "object_id",
                                                                                                            "event_type",
                                                                                                            "bbd_url",
                                                                                                            "bbd_unique_id",
                                                                                                            "event_main",
                                                                                                            "subject_type")
    subject_df.createOrReplaceTempView("subject_table")

    # 补充客体公司类型(是否上市公司)
    object_df = ss_sql(entry, f"""
        select
          zet.id,
          zet.event_subject,
          zet.subject_id,
          zet.event_object,
          zet.object_id,
          zet.event_type,
          zet.bbd_url,
          zet.bbd_unique_id,
          zet.event_main,
          zet.subject_type,
          it.bbd_qyxx_id,
          it.type,
          1 as object_type
        from subject_table zet
        left join ic_table it
        on zet.object_id=it.bbd_qyxx_id
        """)
    object_df = object_df.withColumn("object_type", get_is_public_company("bbd_qyxx_id", "type")).select("id",
                                                                                                         "event_subject",
                                                                                                         "subject_id",
                                                                                                         "event_object",
                                                                                                         "object_id",
                                                                                                         "event_type",
                                                                                                         "bbd_url",
                                                                                                         "bbd_unique_id",
                                                                                                         "event_main",
                                                                                                         "subject_type",
                                                                                                         "object_type")
    object_df.createOrReplaceTempView("object_table")

    # 得到关联关系最紧密的公司信息
    related_df = ss_sql(entry, f"""
        select
            st.*
        from (
            select
                bbd_qyxx_id,
                company_name,
                associated_name,
                associated_id,
                associated_degree,
                associated_degree_name,
                row_number()over (
                    partition by associated_id
                    order by
                        associated_degree,
                        relation,
                        invest_ratio
                    desc) rank
            from pj.ms_ipo_company_related_party_new
            where dt={newest_dt('ms_ipo_company_related_party_new', entry, 'pj')}
            ) st
        where st.rank=1
        """)
    related_df.createOrReplaceTempView("relation_table")

    # 得到主体的关联公司及其关系,将主体公司名称换为标准公司名称
    subject_ass_df = ss_sql(entry, f"""
        select
          zet.id,
          zet.event_subject as event_subject_short,
          if(rt.associated_name is not null,associated_name,zet.event_subject) as event_subject,          zet.subject_id,
          zet.event_object as event_object_short,
          zet.event_object,
          zet.object_id,
          zet.event_type,
          zet.bbd_url,
          zet.bbd_unique_id,
          zet.event_main,
          zet.subject_type,
          zet.object_type,
          rt.company_name as subject_ass_public_company,
          rt.associated_degree_name as with_event_subject_relation
        from object_table zet
        left join relation_table rt
        on zet.subject_id=rt.associated_id
        """)
    subject_ass_df.createOrReplaceTempView("subject_ass_table")

    # 得到客体的关联公司及其关系，将客体公司名称换为标准公司名称
    # 如果主体、客体ID为空，则对应公司类型为空
    object_ass_df = ss_sql(entry, f"""
        select
          zet.id,
          zet.event_subject_short,
          zet.event_subject,
          zet.subject_id,
          zet.event_object_short,
          if(rt.associated_name is not null,associated_name,zet.event_object) as event_object,            zet.object_id,
          zet.event_type,
          zet.bbd_url,
          zet.bbd_unique_id,
          zet.event_main,
          if(zet.subject_id is not null,zet.subject_type,'') as subject_type,
          if(zet.object_id is not null,zet.object_type,'') as object_type,
          zet.subject_ass_public_company,
          zet.with_event_subject_relation,
          rt.company_name as object_ass_public_company,
          rt.associated_degree_name as with_event_object_relation
        from subject_ass_table zet
        left join relation_table rt
        on zet.object_id=rt.associated_id
        """)
    object_ass_df.createOrReplaceTempView("object_ass_table")

    # 统计每个事件（event_type）+主体+客体的重复次数和对应的url（在zszq_yuqing的bbd_url字段里）
    all_data_df = ss_sql(entry, f"""
        select
          id,
          event_subject_short,
          event_subject,
          subject_id,
          subject_type,
          subject_ass_public_company,
          with_event_subject_relation,
          event_object_short,event_object,
          object_id,object_type,
          object_ass_public_company,
          with_event_object_relation,
          event_type,
          size(
          collect_set(bbd_url) over(
            partition by event_type,subject_id,object_id
            )
          ) event_repeat_count,
          collect_set(bbd_url) over(
            partition by event_type,subject_id,object_id
            ) related_news_url,
          collect_list(bbd_unique_id) over(
            partition by event_type,subject_id,object_id
            ) news_source,
          concat_ws('§',collect_list(event_main) over(
            partition by event_type,subject_id,object_id
            )
          ) event_main
        from object_ass_table
        """)

    # 判断事件类型
    get_event_property_udf = udf(get_event_property, StringType())
    all_data_df = all_data_df.withColumn("event_property", get_event_property_udf(all_data_df.event_type))
    # all_data_df.createOrReplaceTempView("temp_all_table")
    # 补充标准事件主体公司名称
    # ss_sql(ss,f"select ta.id,if(qb.company_name is not null,company_name,event_subject),ta.subject_id,ta.subject_type,ta.subject_ass_public_company,ta.with_event_subject_relation,ta.event_object,ta.object_id,ta.object_type,ta.object_ass_public_company,ta.with_event_object_relation,ta.event_type,ta.event_property,ta.event_repeat_count,ta.related_news_url,ta.news_source from temp_all_table ta left join dw.qyxx_basic qb on ta.subject_id=qb.bbd_qyxx_id where qb.dt={newest_dt('qyxx_basic')} ")

    all_data_result_df = all_data_df.dropDuplicates(["event_type", "subject_id", "object_id"])
    all_data_result_df.createOrReplaceTempView("all_data_table")

    # 将今天时间格式化成指定形式
    today = entry.version
    # 列出要存入最终结果表的所有字段
    all_data_fields = ["id", "event_subject_short", "event_subject", "subject_id", "subject_type",
                       "subject_ass_public_company", "with_event_subject_relation", "event_object_short",
                       "event_object", "object_id", "object_type", "object_ass_public_company",
                       "with_event_object_relation", "event_type", "event_property", "event_repeat_count",
                       "related_news_url", "news_source", "event_main"]

    # 将结果存入pj.ms_ipo_public_sentiment_events
    ss_sql(entry, f"""
        insert overwrite table
            pj.ms_ipo_public_sentiment_events_new partition(dt={today})
        select
            {', '.join(all_data_fields)}
        from all_data_table
        """)

    # 只导出利空事件
    all_data_result_new_df = ss_sql(entry, f"""
        select
            {', '.join(all_data_fields)}
        from all_data_table
        where event_property='利空事件'
        """)
    # 将array<string>转换为string，否则存不进csv
    result_df = all_data_result_new_df.withColumn("related_news_url",
                                                  all_data_result_df.related_news_url.cast(StringType())).withColumn(
        "news_source", all_data_result_df.news_source.cast(StringType()))

    result_df.createOrReplaceTempView("result_df_temp")
    # 列出要存入csv的所有字段
    result_all_data_fields = [
        "id as `舆情事件ID`",
        "event_subject_short as `公司1简称`",
        "event_subject as `公司1`",
        "subject_id as `公司1 ID`",
        "subject_type as `公司1 类型`",
        "subject_ass_public_company as `公司1 关联上市公司`",
        "with_event_subject_relation as `与公司1的关系`",
        "event_object_short as `公司2简称`",
        "event_object as `公司2`",
        "object_id as `公司2 ID`",
        "object_type as `公司2类型`",
        "object_ass_public_company as `公司2 关联上市公司`",
        "with_event_object_relation as `与公司2的关系`",
        "event_type as `事件类型`",
        "event_property as `事件属性`",
        "event_repeat_count as `事件热度`",
        "'网页舆情' as `来源类型`",
        "related_news_url as `事件网址`",
        "news_source as `新闻来源`",
        "event_main as `事件原文`"
    ]
    result_df_end = ss_sql(entry, f"""
        select
            {', '.join(result_all_data_fields)}
        from result_df_temp
        """)
    # 将结果存入csv
    # entry.cfg_mgr.hdfs.get_input_path('hdfs', 'hdfs_root_path')
    OUTPUT_HDFS_PATH = entry.cfg_mgr.hdfs.get_result_path("hdfs-biz", "yq_v1_data")
    # OUTPUT_HDFS_PATH = f"{OUTPUT_HDFS_PATH}//f'yq_v1_{entry.version}.csv"
    result_df_end.repartition(1).write.csv(f"{OUTPUT_HDFS_PATH}", header=True, mode='overwrite')

    send = SendEmail(entry=entry,
                     module='yq_v1',
                     receivers=[['刘豪', 'liuhao@bbdservice.com'], ['李婷', 'liting@bbdservice.com'],
                                ['侯冠宇', 'houguanyu@bbdservice.com']],
                     cc_receivers=[]
                     )

    send.send_email_all()

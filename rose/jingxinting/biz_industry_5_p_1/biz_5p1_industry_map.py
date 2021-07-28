#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 经信厅5+1产业图谱
"""
from pyspark.sql import SparkSession, DataFrame
import datetime
import os

from whetstone.core.entry import Entry
from jingxinting.biz_base_data.basic_data_proxy import BasicDataProxy
from jingxinting.proj_common.data_output_util import ResultOutputUtil


def sql_excute_newtable_tohdfs(
        sql,
        entry: Entry,
        newtablename=None,
        paquest_path=None,
        repartition_number=500,
        is_cache=False
):
    '''
        提供执行并打印sql，将结果生成临时表和写入hdfs的方法
    '''

    entry.logger.info(f'当前执行sql为:\n{sql}')

    df: DataFrame = entry.spark.sql(sql)

    if is_cache:
        df.cache()

    if newtablename:
        df.createOrReplaceTempView(f'{newtablename}')
        entry.logger.info(f'生成临时表名为:{newtablename}')

    if paquest_path:
        df.repartition(repartition_number).write.parquet(paquest_path, mode='overwrite')
        # save_text_to_hdfs(df=df, repartition_number=repartition_number, path=paquest_path)
        entry.logger.info(f'写入hdfs地址为{paquest_path}')
        return paquest_path

    return df


# 计算产业图谱指标
def excute_sql(entry: Entry) -> DataFrame:
    sql = f"""
    select 
         {entry.version} as static_month,
        ind_16p1,
        parent_tag,
        tag, 
        tag_level,
        count(distinct bbd_qyxx_id) as comapny_cnt
    from company_tags 
    where nvl(ind_16p1,'') <> ''
    and nvl(bbd_qyxx_id,'') <> ''
    group by 
        ind_16p1,
        parent_tag,
        tag, 
        tag_level
    """
    return sql_excute_newtable_tohdfs(entry=entry, sql=sql)


def out_data(entry: Entry):
    # 获取数据源表
    s = BasicDataProxy(entry=entry)
    basic_df = s.get_tags_info_df()
    basic_df.createOrReplaceTempView('company_tags')

    # 进行逻辑计算
    df = excute_sql(entry=entry)

    # 输出结果到结果表
    r = ResultOutputUtil(entry=entry, table_name='monthly_info_company_tags', month_value_fmt='%Y-%m')
    r.save(df)


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):
    out_data(entry=entry)

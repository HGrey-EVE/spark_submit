#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import datetime

from pyspark.sql.dataframe import DataFrame
from whetstone.core.entry import Entry

from zg11.proj_common.common_util import save_text_to_hdfs
from zg11.proj_common.hive_util import HiveUtil

"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 

Date: 2021/7/6 14:20
"""


def get_basic_data(entry: Entry):
    # dir1 = os.path.join(entry.cfg_mgr.hdfs.get_tmp_path('hdfs', 'hdfs_input_path'), 'zszq_yuqing')
    # entry.spark.read(f'{dir1}').createOrReplaceTempView('zg11_zszq_yuqing')

    # 读取事件库现有事件名称维表
    dir2 = os.path.join(entry.cfg_mgr.hdfs.get_fixed_path('hdfs', 'hdfs_input_path'), 'zszq_event_detail.csv')
    entry.spark.read.csv(path=f'{dir2}', header=True).createOrReplaceTempView('yuqing_event_vb')


    # 读取从mysql导出的数据（招商证券舆情和事件表）
    hive_table_init(spark=entry.spark, tablename='zg11_zszq_yuqing', database='pj')

    # 上市公司的数据
    hive_table_init(spark=entry.spark, tablename='qyxx_jqka_ipo_basic')
    now_date = datetime.datetime.now().strftime('%Y-%m-%d')
    if now_date[6:] > '04-30':
        report_date = str(int(now_date[0:4]) - 1) + '-12-30'
    else:
        report_date = str(int(now_date[0:4]) - 1) + '-09-30'

    sql = f'''
    
        select
        distinct company_name
        from qyxx_jqka_ipo_basic
        where report_date>='{report_date}'
            and coalesce (bbd_qyxx_id,'') <> ''
            and (substr(stock_code,1,2)='30' or substr(stock_code,1,2)='60' or substr(stock_code,1,3)='000')
    '''

    sql_excute_newtable_tohdfs(sql=sql, entry=entry, newtablename='listed_company')


def excute_index(entry: Entry):
    version = entry.version
    # 每天输出存放的路径
    paquest_path = os.path.join(entry.cfg_mgr.hdfs.get_result_path('hdfs-biz', 'hdfs_yuqing_path'), version)


    sql1 = '''
        select 
            a.bbd_xgxx_id as pub_stmt_resc_cde,
            a.event_subject as prvt_org_cde,
            a.news_title as title, a.main as body,
            a.source_url as link,
            a.news_site as webs_name,
            a.event_type as webs_clsf_cde,
            a.pubdate as issue_time,
            a.ctime  as clct_time
        from zg11_zszq_yuqing a , listed_company b 
        where b.company_name = a.event_subject
        union all 
        select 
            a.bbd_xgxx_id as pub_stmt_resc_cde,
            a.event_object as prvt_org_cde,
            a.news_title as title, a.main as body,
            a.source_url as link,
            a.news_site as webs_name,
            a.event_type as webs_clsf_cde,
            a.pubdate as issue_time,
            a.ctime  as clct_time
        from zg11_zszq_yuqing a , listed_company b 
        where b.company_name = a.event_object
    '''

    sql_excute_newtable_tohdfs(sql=sql1, entry=entry, newtablename='tmp1')

    sql2 = '''
        select  
            a.pub_stmt_resc_cde,
            a.prvt_org_cde,
            a.title, a.body,
            a.link,
            a.webs_name,
            b.event_code as webs_clsf_cde,
            a.issue_time,
            a.clct_time
        from tmp1 a 
        left join 
        yuqing_event_vb b 
        on a.webs_clsf_cde = b.event_name
    '''

    df = sql_excute_newtable_tohdfs(sql=sql2, entry=entry)
    df.fillna('').repartition(1).write.json(path=paquest_path, mode='overwrite')


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):
    get_basic_data(entry=entry)
    excute_index(entry=entry)


def hive_table_init(
        spark,
        tablename: str,
        new_table=None,
        database='dw',
        dateNo=None
):
    """
    初始化hive表
    :param spark:
    :param tablename:需要初始化的hive表
    :param new_table:新建的临时表名(默认和原hive表同名)
    :param database:
    :param dateNo: 在某个月的最新分区,注意日期格式要和分区格式一致，eg:201901,2019-01
    """

    if new_table is None:
        new_table = tablename

    if dateNo is None:
        spark.sql('''
            select a.* from {table} a  where dt = '{new_par}'
        '''.format(new_par=HiveUtil.newest_partition(spark, f'{database}.{tablename}'),
                   table=f"{database}.{tablename}")) \
            .createOrReplaceTempView(f'{new_table}')
    else:
        spark.sql('''
             select a.* from {table} a  where dt = '{new_par}'
        '''.format(new_par=HiveUtil.newest_partition_by_month(spark, table=f'{database}.{tablename}',
                                                              in_month_str=str(dateNo)),
                   table=f'{database}.{tablename}')).createOrReplaceTempView(f'{new_table}')


'''
    提供执行并打印sql，将结果生成临时表和写入hdfs的方法
'''


def sql_excute_newtable_tohdfs(
        sql,
        entry: Entry,
        newtablename=None,
        paquest_path=None,
        repartition_number=1,
        is_cache=False
):
    entry.logger.info(f'当前执行sql为:\n{sql}')

    df: DataFrame = entry.spark.sql(sql)

    if is_cache:
        df.cache()

    if newtablename:
        df.createOrReplaceTempView(f'{newtablename}')
        entry.logger.info(f'生成临时表名为:{newtablename}')

    if paquest_path:
        df.repartition(repartition_number).write.json(path=paquest_path, mode='overwrite')
        # df.repartition(repartition_number).write.parquet(f'{paquest_path}', mode='overwrite')
        # save_text_to_hdfs(df=df.rdd, repartition_number=repartition_number, path=paquest_path)
        entry.logger.info(f'写入hdfs地址为{paquest_path}')

    return df

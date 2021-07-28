#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouchao@bbdservice.com
:Date: 2021-04-14 16:08
"""
import json
import os
import traceback
from datetime import date, timedelta, datetime
from whetstone.core.entry import Entry
from yunjian.proj_common.hive_util import HiveUtil
from pyspark.sql import SparkSession, Row, DataFrame
from yunjian.proj_common.hbase_util import HbaseWriteHelper
from yunjian.proj_common.common_util import save_text_to_hdfs
from yunjian.proj_common.date_util import DateUtils


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    """
    程序主入口,配置初始化和业务逻辑入口
    """
    now_date = DateUtils.now2str(fmt='%Y%m%d')
    spark:SparkSession = entry.spark
    # 项目共有配置
    hive_table = entry.cfg_mgr.get("hive", "database")
    basic_path = entry.cfg_mgr.get("hdfs", "hdfs_output_path")
    res_path = basic_path + 'index_9/' + now_date
    hfile_absolute_path = basic_path + 'tmp' + '/index_9_hfile'
    entry.logger.info(f"配置信息hive_db:{hive_table}, hdfs_output_path:{basic_path}")
    # 模块是有配置
    table_name = entry.cfg_mgr.get("hbase", "table_name") + now_date
    family_name = entry.cfg_mgr.get("hbase", "family_name")
    hbase_columns = entry.cfg_mgr.get("hbase", "hbase_columns")
    meta_table_name = entry.cfg_mgr.get("hbase", "meta_table_name")
    meta_row_key = entry.cfg_mgr.get("hbase", "meta_row_key")
    # 获取相关表最新dt
    ratio_path_shareholder_dt = HiveUtil.newest_partition(spark, f"{hive_table}.ratio_path_shareholder")
    qyxx_basic_dt = HiveUtil.newest_partition(spark, f"{hive_table}.qyxx_basic")
    ratio_path_company_dt = HiveUtil.newest_partition(spark, f"{hive_table}.ratio_path_company")
    qyxx_gdxx_dt = HiveUtil.newest_partition(spark, f"{hive_table}.qyxx_gdxx")
    entry.logger.info(f"ratio_path_shareholder:{ratio_path_shareholder_dt}, qyxx_basic:{qyxx_basic_dt}, "
                      f"ratio_path_company:{ratio_path_company_dt}, qyxx_gdxx:{qyxx_gdxx_dt}")

    # 指标计算
    entry.logger.info(f"开始计算指标")
    _rdd = get_index_data(entry, spark, hive_table, ratio_path_shareholder_dt, qyxx_basic_dt, ratio_path_company_dt, qyxx_gdxx_dt)
    entry.logger.info(f"指标计算完成")
    # 数据写入hdfs
    entry.logger.info(f"开始写入hdfs:{res_path}")
    save_text_to_hdfs(_rdd, res_path, repartition_number=10, force_repartition=True)
    entry.logger.info("写入hdfs完成")
    # 数据写入hbase
    entry.logger.info(f"写入hbase:{table_name}")
    load_to_hbase(entry,table_name,family_name,hfile_absolute_path,res_path,hbase_columns,meta_table_name,meta_row_key)
    entry.logger.info(f"写入hbase成功")


def load_to_hbase(entry,table_name,family_name, hfile_absolute_path,index_path,hbase_columns,meta_table_name,meta_row_key):
    """
    数据导入hbase
    """
    hbase_write_helper = HbaseWriteHelper(
        entry.logger,
        table_name,
        family_name,
        "",
        hfile_absolute_path=hfile_absolute_path,
        source_data_absolute_path=index_path,
        source_csv_delimiter="|",
        hbase_columns=hbase_columns,
        meta_table_name=meta_table_name,
        meta_row=meta_row_key
    )
    try:
        entry.logger.info(f"begin load table {table_name} to hbase")
        hbase_write_helper.exec_write()
        entry.logger.info(f"load table {table_name} field hbase success")
    except Exception:
        entry.logger.error(
            f"load table {table_name} to hbase failed:{traceback.format_exc()}")


def get_index_data(entry: Entry, spark, hive_table, ratio_path_shareholder_dt, qyxx_basic_dt, ratio_path_company_dt, qyxx_gdxx_dt):
    """
    获取业务逻辑指标
    """
    entry.logger.info(f"开始计算最终受益人指标")
    shareholder = spark.sql(f"""
                select
                    R.bbd_qyxx_id,
                    L.shareholder_id,
                    1 as finnal_receiptor 
                from
                (select
                    shareholder_cid as shareholder_id,
                    bbd_qyxx_id,
                    shareholder as shareholder_name,
                    shareholder_type,
                    dt
                from {hive_table}.ratio_path_shareholder
                where dt = {ratio_path_shareholder_dt}
                    and is_ultimate=1 and percent>0.25
                    and bbd_qyxx_id != 'null' and bbd_qyxx_id != '' and bbd_qyxx_id != 'NULL' and bbd_qyxx_id is not null
                    and shareholder_cid != 'null' and shareholder_cid != '' and shareholder_cid != 'NULL' and shareholder_cid is not null
                ) L
                join
                (select
                    *
                from {hive_table}.qyxx_basic
                where dt = {qyxx_basic_dt}
                    and bbd_qyxx_id != 'null' and bbd_qyxx_id != '' and bbd_qyxx_id != 'NULL' and bbd_qyxx_id is not null
                ) R
                on L.bbd_qyxx_id = R.bbd_qyxx_id
        """).distinct()

    # 实际受益人
    entry.logger.info(f"开始计算实际受益人指标")
    ultimate = spark.sql(f"""
            select
                    R.bbd_qyxx_id,
                    L.shareholder_id,
                    1 as receiptor 
                from
                (select 
                    shareholder_cid as shareholder_id,
                    bbd_qyxx_id,
                    shareholder as shareholder_name,
                    shareholder_type,
                    dt
                from {hive_table}.ratio_path_shareholder
                    where dt = {ratio_path_shareholder_dt}
                    and is_controller=1 
                    and is_ultimate=1
                    and bbd_qyxx_id != 'null' and bbd_qyxx_id != '' and bbd_qyxx_id != 'NULL' and bbd_qyxx_id is not null
                    and shareholder_cid != 'null' and shareholder_cid != '' and shareholder_cid != 'NULL' and shareholder_cid is not null
                ) L
                inner join
                (select
                    *
                from {hive_table}.qyxx_basic
                    where dt = {qyxx_basic_dt}
                    and bbd_qyxx_id != 'null' and bbd_qyxx_id != '' and bbd_qyxx_id != 'NULL' and bbd_qyxx_id is not null
                ) R
                on L.bbd_qyxx_id = R.bbd_qyxx_id
        """).distinct()

    # 实际控制权
    entry.logger.info(f"开始计算实际受益人指标")
    controller = spark.sql(f"""
            select
                    R.bbd_qyxx_id,
                    L.shareholder_id,
                    1 as controller 
                from
                (select
                    shareholder_cid as shareholder_id,
                    bbd_qyxx_id,
                    shareholder as shareholder_name,
                    shareholder_type,
                    dt
                from {hive_table}.ratio_path_company
                where dt = {ratio_path_company_dt}
                    and percent>=0.05
                    and bbd_qyxx_id != 'null' and bbd_qyxx_id != '' and bbd_qyxx_id != 'NULL' and bbd_qyxx_id is not null
                    and shareholder_cid != 'null' and shareholder_cid != '' and shareholder_cid != 'NULL' and shareholder_cid is not null
                ) L
                inner join
                (select
                    *
                from {hive_table}.qyxx_basic
                    where dt = {qyxx_basic_dt}
                    and bbd_qyxx_id != 'null' and bbd_qyxx_id != '' and bbd_qyxx_id != 'NULL' and bbd_qyxx_id is not null
                ) R
                on L.bbd_qyxx_id = R.bbd_qyxx_id
        """).distinct()

    # 对外投资数
    entry.logger.info(f"开始计算对外投资数指标")
    gd_count = spark.sql(f"""
            select concat(concat(L.bbd_qyxx_id,  "_"), L.shareholder_id) as qy_gd,  
                L.bbd_qyxx_id, L.shareholder_type, L.shareholder_id, R.qy_num
                from 
            (
                 select * 
                    from {hive_table}.qyxx_gdxx
                where dt={qyxx_gdxx_dt}
                    and bbd_qyxx_id != 'null' and bbd_qyxx_id != '' and bbd_qyxx_id != 'NULL' and bbd_qyxx_id is not null
                    and shareholder_id != 'null' and shareholder_id != '' and shareholder_id != 'NULL' and shareholder_id is not null
            ) L left join
            (
                select shareholder_id, count(distinct bbd_qyxx_id) as qy_num
                    from (
                select * 
                    from {hive_table}.qyxx_gdxx
                where dt={qyxx_gdxx_dt}
                    and bbd_qyxx_id != 'null' and bbd_qyxx_id != '' and bbd_qyxx_id != 'NULL' and bbd_qyxx_id is not null
                    and shareholder_id != 'null' and shareholder_id != '' and shareholder_id != 'NULL' and shareholder_id is not null)
                group by shareholder_id
            ) R
            on L.shareholder_id=R.shareholder_id
        """).distinct()

    res_rdd = gd_count.join(shareholder, ["bbd_qyxx_id", "shareholder_id"], "left") \
        .fillna(0) \
        .join(ultimate, ["bbd_qyxx_id", "shareholder_id"], "left") \
        .fillna(0) \
        .join(controller, ["bbd_qyxx_id", "shareholder_id"], "left") \
        .fillna(0).rdd \
        .flatMap(prepare_for_hbase)

    return res_rdd.distinct()


def prepare_for_hbase(row: Row):
    res_list = []
    tmp = row.asDict(True)
    dic = dict()
    qy_gd = tmp['qy_gd']
    dic['finnal_receiptor'] = tmp['finnal_receiptor']
    dic['receiptor'] = tmp['receiptor']
    dic['controller'] = tmp['controller']
    res_list.append(qy_gd + '|' + json.dumps(dic, ensure_ascii=False))
    extend_dict = dict()
    if tmp['shareholder_type'] == 2 and tmp['qy_num'] > 0:
        extend_dict['extend'] = 0
        if tmp['qy_num'] > 0:
            extend_dict['extend'] = 1
        res_list.append(tmp['shareholder_id'] + '|' + json.dumps(extend_dict, ensure_ascii=False))
    return res_list


def post_check(entry: Entry):
    return True
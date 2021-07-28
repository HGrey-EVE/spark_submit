#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-21 15:31
"""
import importlib
import os
import traceback

import requests
from retry import retry
from pyspark.sql import functions as fun

from whetstone.common.exception import FriendlyException
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, Executor
from whetstone.utils.shell import Shell


def generate_external_dataframe_view(entry: Entry):
    # 计算目标公司名单
    company_df = entry.spark.sql(f"""
        select a.corp_code, 
               a.pty_name as company_name, 
               a.bbd_qyxx_id, 
               b.credit_code, 
               b.company_name as bbd_company_name
        from crop_code_v a
        join qyxx_basic_v b
        on a.bbd_qyxx_id = b.bbd_qyxx_id
    """)
    company_df.createOrReplaceTempView('target_company_v')
    Executor.register(name="target_company_v", df_func=company_df, dependencies=['crop_code_v', 'qyxx_basic_v'], cache=True)

    # 计算一度关联方

    relations_drgree1_df = entry.spark.sql(
        f'''
        select distinct a.company_name, a.corp_code, b.company_rel_degree_1
          from target_company_v a
          join (
                select company_name, source_name as company_rel_degree_1
                  from off_line_relations_v
                 where source_isperson=0 and source_degree=1
                union all
                select company_name, destination_name as company_rel_degree_1
                  from off_line_relations_v
                 where destination_isperson=0 and destination_degree=1
                )b
         on a.company_name = b.company_name
         ''').createOrReplaceTempView('all_off_line_relations_v')
    Executor.register(name="all_off_line_relations_v", df_func=relations_drgree1_df, dependencies=['off_line_relations_v'], cache=True)
    # 计算二度关联方
    relations_drgree2_df = entry.spark.sql(
        f'''
            select distinct a.company_name, a.corp_code, b.company_rel_degree_2
              from target_company_v a
              join (
                    select company_name, source_name as company_rel_degree_2
                      from off_line_relations_v
                     where source_isperson=0 and source_degree<=2
                    union all
                    select company_name, destination_name as company_rel_degree_2
                      from off_line_relations_v
                     where destination_isperson=0 and destination_degree<=2
                    )b
             on a.company_name = b.company_name
             ''').createOrReplaceTempView('all_off_line_relations_v')
    Executor.register(name="all_off_line_relations_degree2_v", df_func=relations_drgree2_df, dependencies=['off_line_relations_v'], cache=True)
    for key in ["all_company", "negative_list", "black_list", "yxjnfjrjg_industry"]:
        if key == 'yxjnfjrjg_industry':
            df = entry.spark.read.csv(entry.cfg_mgr.hdfs.get_fixed_path('external_data_path', key),
                                                      header=False,
                                                      sep=',').distinct()
        else:
            df = entry.spark.read.csv(entry.cfg_mgr.hdfs.get_fixed_path('external_data_path', key),
                                  header=True,
                                  sep='\t').distinct()
        Executor.register(name=f"{key}_v", df_func=df, dependencies=[], cache=True)

def get_one_year_date_range(entry: Entry):
    current_date = entry.spark.sql('select current_date() as end_date ').first().end_date
    one_year_ago = entry.spark.sql('select add_months(current_date(), -12) as start_date').first().start_date
    return one_year_ago, current_date


def get_latest_partition(entry: Entry, table):
    database = entry.cfg_mgr.get("hive", "database")
    partitions = entry.spark.sql(f"show partitions {database}.{table}").rdd.map(
        lambda x: x[x.index("=") + 1:]).collect()
    if partitions:
        return partitions[-1]
    else:
        raise FriendlyException(f"{database}.{table} partition is empty")


def generate_pboc_cncc_dataframe_view(entry: Entry):
    # TODO 人民币数据暂未使用，二期使用从数仓取数
    pboc_cncc_beps = entry.spark.read.csv(
        os.path.join(entry.cfg_mgr.hdfs.get_fixed_path('external_data_path', "pboc_cncc"), 'pboc_cncc_beps_2018'),
        header=True, sep='\t')
    pboc_cncc_hvps = entry.spark.read.csv(
        os.path.join(entry.cfg_mgr.hdfs.get_fixed_path('external_data_path', "pboc_cncc"), 'pboc_cncc_hvps_2018'),
        header=True, sep='\t')
    pboc_cncc_ibps = entry.spark.read.csv(
        os.path.join(entry.cfg_mgr.hdfs.get_fixed_path('external_data_path', "pboc_cncc"), 'pboc_cncc_ibps_2018'),
        header=True, sep='\t')
    pboc_cncc = pboc_cncc_beps.unionAll(pboc_cncc_hvps).unionAll(pboc_cncc_ibps)
    Executor.register(name=f"pboc_cncc_v", df_func=pboc_cncc, dependencies=[], cache=True)


def generate_warehouse_dataframe_view(entry: Entry):
    full_partition_tables_dict = {"pboc_corp_lcy": "deal_date",
                                  "pboc_dfxloan_sign": "interest_start_date",
                                  'pboc_delay_pay': "expt_pay_date",
                                  "pboc_delay_recv": "expt_rcv_date",
                                  "pboc_con_exguaran_new": "contractdate",
                                  "pboc_adv_rcv": "expt_imp_date",
                                  "pboc_adv_pay": "expt_exp_date",
                                  "pboc_corp_pay_jn": "pay_date",
                                  "pboc_corp_rcv_jn": "rcv_date",
                                  "pboc_exp_custom_rpt": "exp_date",
                                  "pboc_imp_custom_rpt": "imp_date",
                                  "pboc_corp": "reg_date",
                                  "pboc_impf": "exp_date",
                                  "pboc_expf": "imp_date",
                                  "pboc_frconv_rate": "update_time",
                                  "qyxx_basic": "",
                                  "legal_adjudicative_documents": "",
                                  "pboc_fdi_sh": "upd_date",
                                  "pboc_odi_inv_limit": "start_date",
                                  "off_line_relations": "",
                                  "risk_punishment": "",
                                  "legal_persons_subject_to_enforcement": "",
                                  "legal_dishonest_persons_subject_to_enforcement": "",
                                  "risk_tax_owed": "",
                                  "qyxx_jyyc": "",
                                  "legal_court_notice": "",
                                  "manage_recruit": "",
                                  "manage_tender": "",
                                  "manage_bidwinning": "",
                                  "qyxx_bgxx_merge": "",
                                  "t_acct_rpb":"deal_month"
                                  }
    partition_irrelevant_tables = []
    t_date_dict = {}
    database = entry.cfg_mgr.get("hive", "database")
    for table, date_field in full_partition_tables_dict.items():
        partition = get_latest_partition(entry, table)
        df = entry.spark.sql(f"SELECT * FROM {database}.{table} WHERE dt = {partition}")
        Executor.register(name=f"{table}_v", df_func=df, dependencies=[])
        if date_field:
            df = entry.spark.sql(f"""SELECT substring(max({date_field}), 1, 10) 
                                         FROM {database}.{table} 
                                         WHERE dt = {partition}""")
            t_date = df.collect()[0][0]
            t_date_dict[date_field] = t_date
    for table in partition_irrelevant_tables:
        df = entry.spark.sql(f"SELECT * FROM {database}.{table}")
        Executor.register(name=f"{table}_v", df_func=df, dependencies=[])
    return t_date_dict


@retry(tries=3, delay=5)
def request_backend_index_info(entry: Entry):
    url = entry.cfg_mgr.get("backend", "index_info_url")
    if not url:
        raise FriendlyException("backend index info url empty")
    else:
        res = requests.get(url)
        entry.logger.info(f"backend index info response: \n{res.text}")
        try:
            json_data = res.json()
            status_code, success = json_data.get("statusCode"), json_data.get("success")
            if not (str(status_code) == "1000" and success):
                raise FriendlyException("backend index info response is not SUCCESS status")
            else:
                return [[item.get("code"), item.get("scriptFileUrl")] for item in json_data.get("data", [])]
        except:
            raise FriendlyException("backend index info response is not JSON format")


@retry(tries=3, delay=5)
def download_backend_index_py(entry: Entry, name, url):
    try:
        res = requests.get(url=url)
        with open(f"./{entry.cfg_mgr.get('backend', 'index_py_folder')}/{name}.py",
                  "wb") as code:
            code.write(res.content)
        return True
    except:
        entry.logger.error(f"backend index py download failed, error:\n{traceback.format_exc()}")
        return FriendlyException("backend index py download failed")


def register_backend_index_py(entry: Entry, name):
    py_python_path = entry.cfg_mgr.get("backend", "index_py_python_path")
    paths = py_python_path.split(".")
    paths.append(name)
    try:
        importlib.import_module(".".join(paths))
    except:
        entry.logger.error(f"backend index py import failed, error:\n{traceback.format_exc()}")
        raise FriendlyException("backend index py import failed")


def load_backend_index_py(entry: Entry):
    indices = request_backend_index_info(entry)
    index_pool_name = eval(entry.cfg_mgr.get("index_poll", "index_list"))
    index_pool_list = []
    index_list = []
    for name, url in indices:
        if name in index_pool_name:
            index_pool_list.append(name)
        else:
            index_list.append(name)
            download_backend_index_py(entry, name, url)
            register_backend_index_py(entry, name)
    return index_list, index_pool_list

def get_index_pool_df(entry: Entry, index_name_list):
    database = entry.cfg_mgr.get("hive", "database")
    partition = get_latest_partition(entry, 'index_pool')
    full_columns = index_name_list.append('corp_code')
    df =  entry.spark.sql(f"SELECT {','.join(full_columns)} FROM {database}.index_pool WHERE dt = {partition}")
    return df

def main(entry: Entry):
    database = entry.cfg_mgr.get("hive", "database")
    t_date_dict = generate_warehouse_dataframe_view(entry)
    param = Parameter(**{
        "database": database,
        "tmp_date_dict": t_date_dict
    })
    indices, index_pool_list = load_backend_index_py(entry)
    entry.logger.info(f"indices from backend:{indices}")
    generate_external_dataframe_view(entry)
    index_pool_df = get_index_pool_df(entry, index_pool_list)
    ret_json = Executor.execute(entry, param, entry.logger, *indices)
    failed = [idx for idx, rst in ret_json.items() if idx in indices and not rst]
    entry.logger.warning(f"indices compute failed:{failed}")
    rst_df = Executor.merge(entry,
                            target_df=Executor.df("target_company_v"),
                            ext_index_df=index_pool_df,
                            target_columns=["corp_code", "company_name", 'bbd_qyxx_id',
                                            'credit_code', 'bbd_company_name'],
                            indices=indices,
                            join_columns=["corp_code"],
                            hdfs_tmp_path=entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "index_path", "index"),
                            slice_step_num=5,
                            partition_num=1000)
    rst_df.createOrReplaceTempView('final_index_result_v')
    final_result = entry.spark.sql(
        f'''
            select a.*,  b.esdate, b.regcap_amount, b.company_province, b.company_industry, b.company_type, 
                   substr(b.company_county, 1, 4) as region_code, substr(c.safecode, 1, 4) as safecode, d._c2 as industry_code,
                   b.frname 
              from final_index_result_v a
              left join qyxx_basic_v b
             on a.bbd_qyxx_id = b.bbd_qyxx_id
              left join pboc_corp_v c
             on a.corp_code = c.corp_code
              left join yxjnfjrjg_industry_v d
             on a.corp_code = d._c0 and a.company_name = d._c1
             where a.bbd_qyxx_id is not null 
             and region_code is not null 
             and safecode is not null
            ''').drop_duplicates(['company_name', 'corp_code', 'bbd_qyxx_id', 'bbd_company_name', 'credit_code'])

    final_result.withColumn("dt", fun.lit(entry.version)) \
        .write.saveAsTable('indices', format='orc', mode='append', partitionBy='dt')

    # TODO
    # 导出指标池数据   Done(袁玲洁)
    # 融合指标池数据   Done
    # 写出Hive表dw.indices带时间dt字段 Done
    # export_goods_ct指标，定义表名为 commodity_tax_rebate_rate_v 字段 终止日期（end_date） 起始日期(start_date) 退税率(tax_rebate_rate) 海关商品码(goods_code)
    # 确认company_type对应码表，company_type_v 字段  code码值（code_value）code对应中文名（code_desc） 指标business_type  Done（不需要）
    # 单元测试基本语法测试通过
    # 迁移一期未使用指标至单独文件夹phase_1_archive    Done
    # generate_external_dataframe_view 导入外部表数据部分迁移至 从Hive读取 Done
    # all_off_line_relations_degree2_v Done
    # 准备azkban 任务命令
    # 调整target_company_v Done


def pre_check(entry: Entry):
    external_data_path_cfgs = entry.cfg_mgr.config.options("external_data_path")
    codes = [(entry.cfg_mgr.hdfs.get_fixed_path('external_data_path', option),
              Shell(entry.logger) \
              .build(f"hadoop fs -test -f {entry.cfg_mgr.hdfs.get_fixed_path('external_data_path', option)}") \
              .execute() \
              .return_code) for option in external_data_path_cfgs]
    file_not_exist = [x[0] for x in codes if x[1] != 0]
    entry.logger.error(f"external data file not exist, {file_not_exist}") if file_not_exist else None
    return bool(len(file_not_exist))


def post_check(entry: Entry):
    return True
#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: node_7_13
:Author: zhouchao@bbdservice.com
:Date: 2021-05-26 4:40 PM
:Version: v.1.0
:Description:
"""
import copy
import os
import re

from scwgj.graph_subject_data.extract_core import GraphDataExtractTemplate, TableMetaData
from scwgj.proj_common.hive_util import HiveUtil
from pyspark.sql import DataFrame, Row


class VertexPhone(GraphDataExtractTemplate):
    """
    主体-9: 电话号码
    """

    TABLE_NAME = 'vertex_phone'
    TABLE_COLUMNS = ['ID', 'phonenumber']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_1_name = f'{self.hive_dw_db_name}.pboc_corp'
        table_2_name = f'{self.hive_dw_db_name}.pboc_corp_buy'
        table_3_name = f'{self.hive_dw_db_name}.pboc_corp_lcy'
        table_4_name = f'{self.hive_dw_db_name}.pboc_corp_pay_jn'
        table_5_name = f'{self.hive_dw_db_name}.pboc_corp_pay_rcv'
        table_1_dt = HiveUtil.newest_partition(self.spark, table_1_name)
        table_2_dt = HiveUtil.newest_partition(self.spark, table_2_name)
        table_3_dt = HiveUtil.newest_partition(self.spark, table_3_name)
        table_4_dt = HiveUtil.newest_partition(self.spark, table_4_name)
        table_5_dt = HiveUtil.newest_partition(self.spark, table_5_name)

        tmp = self.spark.sql(f"""
                select md5(concat_ws('-', {VertexPhone.TABLE_NAME}, phonenumber)) as ID, 
                    phonenumber
                from
                    (select tel as phonenumber
                    from {table_1_name} 
                    where dt='{table_1_dt}'
                    union all
                    select filler_tel as phonenumber
                    from {table_2_name} 
                    where dt='{table_2_dt}'
                    union all
                    select filler_tel as phonenumber
                    from {table_3_name} 
                    where dt='{table_3_dt}'
                    union all
                    select operator_tel as phonenumber
                    from {table_4_name} 
                    where dt='{table_4_dt}'
                    union all
                    select rpt_tel as phonenumber
                    from {table_4_name} 
                    where dt='{table_4_dt}'
                    union all
                    select operator_tel as phonenumber
                    from {table_5_name} 
                    where dt='{table_5_dt}'
                    union all
                    select rpt_tel as phonenumber
                    from {table_5_name} 
                    where dt='{table_5_dt}')
                group by ID, phonenumber
                """)

        detail_rdd = tmp.rdd.filter(self.is_phone)
        df = self.spark.createDataFrame(detail_rdd)
        return df

    def is_phone(self, data: Row) -> bool:
        if not data['phonenumber']:
            return False
        else:
            if re.findall('^[1][3-9][0-9]{9}$', data['phonenumber']):
                return True
            else:
                return False

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='',
            destination_table_name=VertexPhone.TABLE_NAME,
            destination_table_columns=VertexPhone.TABLE_COLUMNS,
            primary_keys=['ID'],
            has_update_data=False
        )


class VertexBranch(GraphDataExtractTemplate):
    """
    主体-10: 金融机构
    """

    TABLE_NAME = 'vertex_branch'
    TABLE_COLUMNS = ['branch_code', 'pty_name', 'corp_code', 'license_no',
                     "bank_name", "safecode", "bank_attr_code", "bank_attr_desc", "region_code", "address"]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_bank_branch'
        dt = HiveUtil.newest_partition(self.spark, table_name)

        tmp = self.spark.sql(f"""
                    select * 
                    from {table_name}
                    where dt='{dt}'
                """)
        detail_rdd = tmp.rdd.map(lambda x: self.count_nu(x))
        self.spark.createDataFrame(detail_rdd).createOrReplaceTempView('temp_v')
        df = self.spark.sql(f"""
                    select {','.join(self.TABLE_COLUMNS)} 
                    from
                        (select *, 
                            row_number over(partition by branch_code over by nu_count) rn
                        from temp_v)
                    where rn=1
                """)

        return df

    def count_nu(self, data: Row) -> Row:
        nu_count = 0
        for col in self.TABLE_COLUMNS:
            if not eval('data.{col}'.format(col=col)):
                nu_count += 1

        return Row(branch_code=data.branch_code,
                   pty_name=data.pty_name,
                   corp_code=data.corp_code,
                   license_no=data.license_no,
                   bank_name=data.bank_name,
                   safecode=data.safecode,
                   bank_attr_code=data.bank_attr_code,
                   bank_attr_desc=data.bank_attr_desc,
                   region_code=data.region_code,
                   address=data.address,
                   nu_count=nu_count)

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='',
            destination_table_name=VertexBranch.TABLE_NAME,
            destination_table_columns=VertexBranch.TABLE_COLUMNS,
            primary_keys=['branch_code'],
            has_update_data=False
        )


class VertexCustom(GraphDataExtractTemplate):
    """
    主体-11: 报关单号
    """

    TABLE_NAME = 'vertex_custom'
    tab_col = ['custom_rpt_no',  "custom_date", "contract_no", "trade_country_code",
               "custom_trade_mode_code", "custom_trade_mode_name", "deal_ccy_code",
               "cdf_deal_amt_usd", "custom_rpt_status_code"]

    TABLE_COLUMNS = copy.deepcopy(tab_col)
    TABLE_COLUMNS.append('custom_type')

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_1_name = f'{self.hive_dw_db_name}.pboc_expf'
        table_2_name = f'{self.hive_dw_db_name}.pboc_impf'
        table_3_name = f'{self.hive_dw_db_name}.pboc_exp_custom_rpt'
        table_4_name = f'{self.hive_dw_db_name}.pboc_imp_custom_rpt'
        table_1_dt = HiveUtil.newest_partition(self.spark, table_1_name)
        table_2_dt = HiveUtil.newest_partition(self.spark, table_2_name)
        table_3_dt = HiveUtil.newest_partition(self.spark, table_3_name)
        table_4_dt = HiveUtil.newest_partition(self.spark, table_4_name)
        col_str = ','.join(self.tab_col)
        df = self.spark.sql(f"""
                select {','.join(self.TABLE_COLUMNS)}
                from
                    (select {col_str}, `出口` as  custom_type
                    from {table_1_name} 
                    where dt='{table_1_dt}'
                    union all
                    select {col_str}, `进口` as  custom_type
                    from {table_2_name} 
                    where dt='{table_2_dt}'
                    union all
                    select {col_str}, `出口` as  custom_type
                    from {table_3_name} 
                    where dt='{table_3_dt}'
                    union all
                    select {col_str}, `进口` as  custom_type
                    from {table_4_name} 
                    where dt='{table_4_dt}')
                group by {','.join(self.TABLE_COLUMNS)}
                """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='',
            destination_table_name=VertexCustom.TABLE_NAME,
            destination_table_columns=VertexCustom.TABLE_COLUMNS,
            primary_keys=['custom_rpt_no'],
            has_update_data=False
        )


class VertexMerch(GraphDataExtractTemplate):
    """
    主体-12: 尚品
    """

    TABLE_NAME = 'vertex_merch'
    TABLE_COLUMNS = ['UID',  "merch_name"]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        input_path = self.entry.cfg_mgr.hdfs.get_input_path('graph-biz', 'merch_input_path')
        os.system(f'hadoop fs -put ../lib/merch.csv {input_path}')
        table_1_name = f'{self.hive_dw_db_name}.pboc_expf'
        table_2_name = f'{self.hive_dw_db_name}.pboc_impf'
        table_1_dt = HiveUtil.newest_partition(self.spark, table_1_name)
        table_2_dt = HiveUtil.newest_partition(self.spark, table_2_name)

        self.apark.read.csv(input_path, header=True, SEP='\t').createOrReplaceTempView('merch')
        df = self.spark.sql(f"""
                select md5(concat_ws('-', {VertexMerch.TABLE_NAME}, L.merch_code)) as UID, r.merch_name
                from
                    (select  merch_code
                    from {table_1_name} 
                    where dt='{table_1_dt}'
                    union all
                    select merch_code
                    from {table_2_name} 
                    where dt='{table_2_dt}'
                    group by merch_code
                    ) L
                left join 
                merch R
                on substring(L.merch_code, 1, 2)=R.merch_code
                """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='vertex_merch',
            destination_table_name=VertexMerch.TABLE_NAME,
            destination_table_columns=VertexMerch.TABLE_COLUMNS,
            primary_keys=['UID'],
            has_update_data=False
        )


class VertexGuarantee(GraphDataExtractTemplate):
    """
    主体-13: 担保
    """

    TABLE_NAME = 'vertex_guarantee'
    tab_col = ['custom_rpt_no', "custom_date", "contract_no", "trade_country_code",
               "custom_trade_mode_code", "custom_trade_mode_name", "deal_ccy_code",
               "cdf_deal_amt_usd", "custom_rpt_status_code"]

    TABLE_COLUMNS = copy.deepcopy(tab_col)
    TABLE_COLUMNS.append('UID')

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_guar_sign'
        table_dt = HiveUtil.newest_partition(self.spark, table_name)
        col_str = ''.join(self.tab_col)
        df = self.spark.sql(f"""
                select  md5(concat_ws('-', {VertexGuarantee.TABLE_NAME}, guar_biz_no)) as ID, {col_str}
                from
                    (select {col_str}, 
                    from {table_name} 
                    where dt='{table_dt}'
                    and (guar_type_code='自身生产经营担保'
                    or guar_type_code='其他融资性担保'
                    or guar_type_code='受信额度担保')
                group by {col_str}
                """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='',
            destination_table_name=VertexCustom.TABLE_NAME,
            destination_table_columns=VertexCustom.TABLE_COLUMNS,
            primary_keys=['UID'],
            has_update_data=False
        )
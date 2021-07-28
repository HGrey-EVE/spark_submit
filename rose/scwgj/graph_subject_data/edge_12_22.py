#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: edge_12_23
:Author: xufeng@bbdservice.com 
:Date: 2021-05-19 4:39 PM
:Version: v.1.0
:Description:
"""
from pyspark.sql import DataFrame

from scwgj.graph_subject_data.extract_core import GraphDataExtractTemplate, TableMetaData
from scwgj.graph_subject_data.node_1_6 import VertexCorp
from scwgj.graph_subject_data.node_1_6 import VertexRmbaccount
from scwgj.graph_subject_data.node_1_6 import VertexFxaccount
from scwgj.graph_subject_data.node_1_6 import VertexTransfer
from scwgj.graph_subject_data.node_1_6 import VertexPerson
from scwgj.proj_common.hive_util import HiveUtil


class EdgeCorpHasFxaccount(GraphDataExtractTemplate):
    """
    边-12: 公司-拥有-外汇账户
    """

    TABLE_NAME = 'edge_corp_has_fxaccount'
    TABLE_COLUMNS = [
        'id', 'startnode', 'endnode'
    ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name1 = f'{self.hive_dw_db_name}.pboc_acctno'
        dt1 = HiveUtil.newest_partition(self.spark, table_name1)
        table_name2 = f'{self.hive_dw_db_name}.pboc_corp_buy'
        dt2 = HiveUtil.newest_partition(self.spark, table_name2)
        table_name3 = f'{self.hive_dw_db_name}.pboc_corp_lcy'
        dt3 = HiveUtil.newest_partition(self.spark, table_name3)
        table_name4 = f'{self.hive_dw_db_name}.pboc_corp_pay_jn'
        dt4 = HiveUtil.newest_partition(self.spark, table_name4)
        table_name5 = f'{self.hive_dw_db_name}.pboc_corp_rcv_jn'
        dt5 = HiveUtil.newest_partition(self.spark, table_name5)
        table_name6 = f'{self.hive_dw_db_name}.t_acct_rpb'
        dt6 = HiveUtil.newest_partition(self.spark, table_name6)
        table_name7 = f'{self.hive_dw_db_name}.t_acct_pay_dtl'
        dt7 = HiveUtil.newest_partition(self.spark, table_name7)
        table_name8 = f'{self.hive_dw_db_name}.t_acct_rcv_dtl'
        dt8 = HiveUtil.newest_partition(self.spark, table_name8)

        table1 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexFxaccount.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpHasFxaccount.TABLE_NAME},R1.uid,R2.fx_accno)) as id,
                R1.uid as startnode,
                R2.fx_accno as endnode
            FROM 
                (SELECT acctno_pty_code as corp_code,acctno as fx_acctno FROM {table_name1} where dt = '{dt1}' and econ_type_code != 400
                union 
                SELECT corp_code,acctno as fx_acctno FROM {table_name2} where dt = '{dt2}' and econ_type_code!=400
                union
                SELECT corp_code,acctno as fx_acctno  FROM {table_name3} where dt = '{dt3}' and econ_type_code!=400
                union
                SELECT corp_code,rmb_acctno as fx_acctno  FROM {table_name3} where dt = '{dt3}' and econ_type_code!=400 and salefx_use_code='022'
                union 
                SELECT corp_code,fx_acctno FROM {table_name4} where dt = '{dt4}' and tx_code!=822030
                union 
                SELECT corp_code,oth_acctno AS fx_acctno FROM {table_name4} where dt = '{dt4}' and tx_code!=822030 and tx_code=929070 and oth_acctno is not null
                union 
                SELECT corp_code,fx_acctno FROM {table_name5} where dt = '{dt5} and tx_code!=822030'
                union 
                SELECT corp_code,oth_acctno AS fx_acctno FROM {table_name5} where dt = '{dt5}' and tx_code!=822030 and tx_code=929070 and oth_acctno is not null
                union 
                SELECT corp_code, acctno AS fx_acctno FROM {table_name6} where dt = '{dt6}' and econ_type_code!=400
                union 
                SELECT payer_name as corp_code, payer_acctno AS fx_acctno FROM {table_name7} where dt = '{dt7}' and is_dom_pay=1 and econ_type_code!=400
                union 
                SELECT rcver_name as corp_code, rcver_acctno AS fx_acctno FROM {table_name7} where dt = '{dt7}' and acct_ccy_code！=“人民币”
                union 
                SELECT rcver_name as corp_code, rcver_acctno AS fx_acctno FROM {table_name7} where dt = '{dt7}' and acct_ccy_code=“人民币” and tx_code!=929070
                union 
                SELECT rcver_name as corp_code, rcver_acctno AS fx_acctno FROM {table_name8} where dt = '{dt8}' and is_dom_pay=1 and econ_type_code!=400
                union 
                SELECT payer_name as corp_code, payer_acctno AS fx_acctno FROM {table_name8} where dt = '{dt8}' and acct_ccy_code！=“人民币”
                union 
                SELECT payer_name as corp_code, payer_acctno AS fx_acctno FROM {table_name8} where dt = '{dt8}' and acct_ccy_code=“人民币” and tx_code!=929070
                ) L
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.corp_code = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.fx_acctno = R2.fx_accno 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_acctno,pboc_corp_buy,pboc_corp_lcy,pboc_corp_pay_jn,pboc_corp_rcv_jn',
            destination_table_name=EdgeCorpHasFxaccount.TABLE_NAME,
            destination_table_columns=EdgeCorpHasFxaccount.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )



class EdgeCorpHasRmbaccount(GraphDataExtractTemplate):
    """
    边-13: 公司-拥有-人民币账户
    """

    TABLE_NAME = 'edge_corp_has_rmbaccount'
    TABLE_COLUMNS = [
        'id', 'startnode', 'endnode'
    ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name1 = f'{self.hive_dw_db_name}.pboc_corp_buy'
        dt1 = HiveUtil.newest_partition(self.spark, table_name1)
        table_name2 = f'{self.hive_dw_db_name}.pboc_corp_lcy'
        dt2 = HiveUtil.newest_partition(self.spark, table_name2)
        table_name3 = f'{self.hive_dw_db_name}.pboc_corp_pay_jn'
        dt3 = HiveUtil.newest_partition(self.spark, table_name3)
        table_name4 = f'{self.hive_dw_db_name}.pboc_corp_rcv_jn'
        dt4 = HiveUtil.newest_partition(self.spark, table_name4)
        table_name5 = f'{self.hive_dw_db_name}.t_acct_pay_dtl'
        dt5 = HiveUtil.newest_partition(self.spark, table_name5)
        table_name6 = f'{self.hive_dw_db_name}.t_acct_rcv_dtl'
        dt6 = HiveUtil.newest_partition(self.spark, table_name6)

        table1 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexRmbaccount.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpHasRmbaccount.TABLE_NAME},R1.uid,R2.rmb_accno)) as id,
                R1.uid as startnode,
                R2.rmb_accno as endnode
            FROM 
                (SELECT corp_code,rmb_acctno FROM {table_name1} where dt = '{dt1}' and econ_type_code != 400
                union 
                SELECT corp_code,rmb_acctno as fx_acctno FROM {table_name2} where dt = '{dt2}' and econ_type_code!=400 and salefx_use_code!='022'
                union 
                SELECT corp_code,oth_acctno as rmb_acctno AS fx_acctno FROM {table_name3} where dt = '{dt3}' and tx_code!=822030 and tx_code=929070 and oth_acctno is not null
                union 
                SELECT corp_code,oth_acctno AS rmb_acctno FROM {table_name4} where dt = '{dt4}' and tx_code!=822030 and tx_code=929070 and oth_acctno is not null
                union 
                SELECT rcver_name as corp_code,rcver_acctno AS rmb_acctno FROM {table_name5} where dt = '{dt5}' and acct_ccy_code=“人民币” and tx_code=929070
                union 
                SELECT payer_name as corp_code,payer_acctno AS rmb_acctno FROM {table_name6} where dt = '{dt6}' and acct_ccy_code=“人民币” and tx_code=929070
                ) L
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.corp_code = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.rmb_accno = R2.rmb_accno 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_buy,pboc_corp_lcy,pboc_corp_pay_jn,pboc_corp_rcv_jn',
            destination_table_name=EdgeCorpHasRmbaccount.TABLE_NAME,
            destination_table_columns=EdgeCorpHasRmbaccount.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeCorpApplyBranch(GraphDataExtractTemplate):
    """
    边-15: 外汇账户-out-交易
    """

    TABLE_NAME = 'edge_corp_apply_branch'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_corp_pay_jn'
        table_2_name = f'{self.hive_dw_db_name}.t_acct_pay_dtl'
        dt_2 = HiveUtil.newest_partition(self.spark, table_2_name)
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexFxaccount.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexTransfer.TABLE_NAME}'  # TODO: add finical tale name

        df_table_1 = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpApplyBranch.TABLE_NAME}, R1.rmb_accno, R2.rptno)) as id,
                R1.rmb_accno as startnode,
                R2.rptno as endnode
            FROM 
                (select oth_acctno,  rptno from {table_name} where x_code = '929070' AND oth_acctno is not null and dt = '{dt}' 
                union 
                select payer_acctno as oth_acctno, rptno from {table_2_name} where is_dom_pay=1 and dt = '{dt2}' 
                ) L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.oth_acctno = R1.rmb_accno 
            LEFT JOIN 
                (select * {table2} where dt = '{self.exec_version}') R2 ON L.rptno = R2.rptno
        """)

        df_table_2 = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpApplyBranch.TABLE_NAME}, R1.rmb_accno, R2.rptno)) as id,
                R1.rmb_accno as startnode,
                R2.rptno as endnode
            FROM 
                (select * from {table_2_name} where is_dom_pay=1 and dt = '{dt}') L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.payer_acctno = R1.rmb_accno 
            LEFT JOIN 
                (select * {table2} where dt = '{self.exec_version}') R2 ON L.rptno = R2.rptno
        """)

        df = df_table_1.union(df_table_2).drop_duplicates(['startnode', 'endnode'])

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_pay_jn',
            destination_table_name=EdgeCorpApplyBranch.TABLE_NAME,
            destination_table_columns=EdgeCorpApplyBranch.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeTransferTnFxaccount(GraphDataExtractTemplate):
    """
    边-16: 交易-in-外汇账户
    """

    TABLE_NAME = 'edge_transfer_in_fxaccount'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_corp_rcv_jn'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table2_name = f'{self.hive_dw_db_name}.t_acct_rcv_dtl'
        dt2 = HiveUtil.newest_partition(self.spark, table2_name)
        table1 = f'{self.hive_biz_db_name}.{VertexTransfer.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexFxaccount.TABLE_NAME}'  # TODO: add finical tale name

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpApplyBranch.TABLE_NAME}, R1.rptno, R2.fx_accno)) as id,
                R1.rptno as startnode,
                R2.fx_accno as endnode
            FROM 
                (select rptno, oth_acctno from {table_name} where dt = '{dt}' and x_code = '929070' AND oth_acctno is not null 
                union 
                select rptno, rcver_acctno as oth_acctno from {table2_name} where dt = '{dt2}'
                ) L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.rptno = R1.rptno 
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.oth_acctno = R2.fx_accno
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_rcv_jn',
            destination_table_name=EdgeTransferTnFxaccount.TABLE_NAME,
            destination_table_columns=EdgeTransferTnFxaccount.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )

class EdgeTransferInRmbaccount(GraphDataExtractTemplate):
    """
    边-17: 交易-in-人民币账户
    """

    TABLE_NAME = 'edge_transfer_in_rmbaccount'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.t_acct_pay_dtl'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexTransfer.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexRmbaccount.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeTransferInRmbaccount.TABLE_NAME}, L.rptno, L.rcver_acctno)) as id,
                R1.rptno as startnode,
                R2.rmb_accno as endnode,   
            FROM 
                (select * from {table_name} where dt = '{dt}' and acct_ccy_code='人民币' and is_dom_pay=1 and tx_code=929070) L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.rptno = R1.rptno
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.rcver_acctno = R2.rmb_acctno
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='t_acct_pay_dtl',
            destination_table_name=EdgeTransferInRmbaccount.TABLE_NAME,
            destination_table_columns=EdgeTransferInRmbaccount.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )

class EdgeRmbaccountOutTransfer(GraphDataExtractTemplate):
    """
    边-18: 人民币账户-out-交易
    """

    TABLE_NAME = 'edge_rmbaccount_out_transfer'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.t_acct_rcv_dtl'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexTransfer.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexRmbaccount.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeTransferInRmbaccount.TABLE_NAME}, L.payer_acctno, L.rptno)) as id,
                R2.rmb_acctno as startnode,
                R2.rmb_acctno as endnode,   
            FROM 
                (select * from {table_name} where dt = '{dt}' and acct_ccy_code='人民币' and is_dom_pay=1 and tx_code=929070) L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.rptno = R1.rmb_acctno
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.payer_acctno = R2.rmb_acctno
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='t_acct_rcv_dtl',
            destination_table_name=EdgeRmbaccountOutTransfer.TABLE_NAME,
            destination_table_columns=EdgeRmbaccountOutTransfer.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )

class EdgeFxaccountLcyRmbaccount(GraphDataExtractTemplate):
    """
    边-19: 外汇账户-结汇-人民币账户
    """

    TABLE_NAME = 'edge_fxaccount_lcy_rmbaccount'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode',
                     'refno', 'salefx_amt_usd', 'tx_code', 'salefx_exrate', 'deal_date', 'salefx_use_desc2'
                     ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_corp_lcy'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexFxaccount.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexRmbaccount.TABLE_NAME}'  # TODO: add finical tale name

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpApplyBranch.TABLE_NAME}, L.refno)) as id,
                R1.rptno as startnode,
                R2.rmb_accno as endnode,
                L.refno,
                L.salefx_amt_usd,
                L.tx_code,
                L.salefx_exrate,
                L.deal_date,
                L.salefx_use_desc2                
            FROM 
                (select * from {table_name} where dt = '{dt}' and salefx_use_code != '022') L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.acctno = R1.fx_accno
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.rmb_acctno = R2.rmb_accno
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_lcy',
            destination_table_name=EdgeFxaccountLcyRmbaccount.TABLE_NAME,
            destination_table_columns=EdgeFxaccountLcyRmbaccount.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeRmbaccountBuyFxaccount(GraphDataExtractTemplate):
    """
    边-20: 人民币账户-购汇-外汇账户
    """

    TABLE_NAME = 'edge_rmbaccount_buy_fxaccount'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode',
                     'refno', 'salefx_amt_usd', 'tx_code', 'salefx_exrate', 'deal_date', 'salefx_use_desc2'
                     ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_corp_buy'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexRmbaccount.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexFxaccount.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpApplyBranch.TABLE_NAME}, L.refno)) as id,
                R1.rptno as startnode,
                R2.rmb_accno as endnode,
                L.refno,
                L.salefx_amt_usd,
                L.tx_code,
                L.salefx_exrate,
                L.deal_date,
                L.salefx_use_desc2                
            FROM 
                (select * from {table_name} where dt = '{dt}') L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.rmb_acctno = R1.rmb_accno
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.acctno = R2.fx_accno
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_buy',
            destination_table_name=EdgeRmbaccountBuyFxaccount.TABLE_NAME,
            destination_table_columns=EdgeRmbaccountBuyFxaccount.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeCompanyInvestCompany(GraphDataExtractTemplate):
    """
    边-21: 公司-投资-公司
    """

    TABLE_NAME = 'edge_company_invest_company'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode', 'invest_ratio', 'invest_amount']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.off_line_relations'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpApplyBranch.TABLE_NAME}, R1.uid, R2.uid, L.ctime)) as id,
                R1.uid as startnode,
                R2.uid as endnode,
                L.invest_ratio,
                L.invest_amount           
            FROM 
                (select * from {table_name} where dt = '{dt}') L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.source_bbd_id = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.destination_bbd_id = R2.corp_code
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_buy',
            destination_table_name=EdgeCompanyInvestCompany.TABLE_NAME,
            destination_table_columns=EdgeCompanyInvestCompany.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )



class EdgePersonExecutiveCompany(GraphDataExtractTemplate):
    """
    边-22: 自然人-高管-公司
    """

    TABLE_NAME = 'edge_person_executive_company'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.off_line_relations'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexPerson.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpApplyBranch.TABLE_NAME}, R1.uid, R2.uid, L.ctime)) as id,
                R1.uid as startnode,
                R2.uid as endnode       
            FROM 
                (select * from {table_name} where dt = '{dt}') L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.source_bbd_id = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.destination_bbd_id = R2.corp_code
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='off_line_relations',
            destination_table_name=EdgePersonExecutiveCompany.TABLE_NAME,
            destination_table_columns=EdgePersonExecutiveCompany.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )
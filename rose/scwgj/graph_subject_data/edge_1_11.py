#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: edge_1_11
:Author: xufeng@bbdservice.com 
:Date: 2021-05-19 3:53 PM
:Version: v.1.0
:Description:
"""
from pyspark.sql import DataFrame

from scwgj.graph_subject_data.extract_core import GraphDataExtractTemplate, TableMetaData
from scwgj.graph_subject_data.node_1_6 import VertexCorp
from scwgj.graph_subject_data.node_1_6 import VertexBranch
from scwgj.graph_subject_data.node_7_13 import VertexGuarantee
from scwgj.graph_subject_data.node_1_6 import VertexOvscorp
from scwgj.graph_subject_data.node_7_13 import VertexPhone
from scwgj.graph_subject_data.node_7_13 import VertexCustom
from scwgj.graph_subject_data.node_7_13 import VertexMerch
from scwgj.proj_common.hive_util import HiveUtil


class EdgeCorpApplyBranch(GraphDataExtractTemplate):
    """
    边-1: 公司－申请担保－金融机构
    """

    TABLE_NAME = 'edge_corp_apply_branch'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_guar_sign'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexBranch.TABLE_NAME}'  # TODO: add finical tale name

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpApplyBranch.TABLE_NAME}, L.guar_biz_no, L.app_code, L.branch_code)) as id,
                R1.uid as startnode,
                R2.uid as endnode
            FROM 
                (select * from {table_name} where dt = '{dt}') L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.app_code = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.branch_code = R2.corp_code 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_guar_sign',
            destination_table_name=EdgeCorpApplyBranch.TABLE_NAME,
            destination_table_columns=EdgeCorpApplyBranch.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )



class EdgeBranchGuaranteeGuarantee(GraphDataExtractTemplate):
    """
    边-2: 金融机构-担保-担保
    """

    TABLE_NAME = 'edge_branch_guarantee_guarantee'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_guar_sign'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexGuarantee.TABLE_NAME}'  # TODO: add finical tale name

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeBranchGuaranteeGuarantee.TABLE_NAME}, L.guar_biz_no, L.app_code, L.branch_code)) as id,
                R1.uid as startnode,
                R2.uid as endnode
            FROM 
                (select * from {table_name} where dt = '{dt}') L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.branch_code = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.guar_biz_no = R2.UID 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_guar_sign',
            destination_table_name=EdgeBranchGuaranteeGuarantee.TABLE_NAME,
            destination_table_columns=EdgeBranchGuaranteeGuarantee.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeGuaranteeGuaranteedOvscorp(GraphDataExtractTemplate):
    """
    边-3: 担保-被担保-境外主体
    """

    TABLE_NAME = 'edge_guarantee_guaranteed_ovscorp'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_guar_sign'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexGuarantee.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexOvscorp.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeBranchGuaranteeGuarantee.TABLE_NAME}, L.guar_biz_no, L.app_code, L.branch_code)) as id,
                R1.uid as startnode,
                R2.uid as endnode
            FROM 
                (select * from {table_name} where dt = '{dt}' and country_code != 'CHN' ) L 
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.guar_biz_no = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.guartee_code = R2.corp_code 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_guar_sign',
            destination_table_name=EdgeGuaranteeGuaranteedOvscorp.TABLE_NAME,
            destination_table_columns=EdgeGuaranteeGuaranteedOvscorp.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )

class EdgeBranchDfxloanCorp(GraphDataExtractTemplate):
    """
    边-4: 金融机构-外汇贷款-公司
    """

    TABLE_NAME = 'edge_branch_dfxloan_corp'
    TABLE_COLUMNS = [
        'id', 'startnode', 'endnode', 'fx_loan_type_name', 'loan_ccy_code',
        'sign_amt_usd', 'interest_start_date', 'mature_date'
    ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_dfxloan_sign'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexGuarantee.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'  # TODO: add finical tale name

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeBranchGuaranteeGuarantee.TABLE_NAME},L.dfxloan_no)) as id,
                R1.uid as startnode,
                R2.uid as endnode,
                fx_loan_type_name as L.fx_loan_type_name,
                loan_ccy_code as L.loan_ccy_code,
                sign_amt_usd as L.sign_amt_usd,
                interest_start_date as L.interest_start_date,
                mature_date as L.mature_date
            FROM 
                (select * from {table_name} where dt = '{dt}') L
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.creditor_id = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.corp_code = R2.corp_code 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_dfxloan_sign',
            destination_table_name=EdgeBranchDfxloanCorp.TABLE_NAME,
            destination_table_columns=EdgeBranchDfxloanCorp.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeOvscorpFDICorp(GraphDataExtractTemplate):
    """
    边-5: 境外主体-FDI-公司
    """

    TABLE_NAME = 'edge_ovscorp_FDI_corp'
    TABLE_COLUMNS = [
        'id', 'startnode', 'endnode', 'invest_ratio', 'inv_amt_usd'
    ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_fdi_sh'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexOvscorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'  # TODO: add finical tale name

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeBranchGuaranteeGuarantee.TABLE_NAME},L.sh_pty_code,L.pty_code,L.upd_date)) as id,
                R1.uid as startnode,
                R2.uid as endnode,
                L.nvest_ratio,
                L.inv_amt_usd
            FROM 
                (select * from {table_name} where dt = '{dt}') L
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.sh_pty_code = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.pty_code = R2.corp_code 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='edge_ovscorp_FDI_corp',
            destination_table_name=EdgeOvscorpFDICorp.TABLE_NAME,
            destination_table_columns=EdgeOvscorpFDICorp.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeCorpODIOvscorp(GraphDataExtractTemplate):
    """
    边-6: 公司-ODI-境外主体
    """

    TABLE_NAME = 'edge_corp_ODI_ovscorp'
    TABLE_COLUMNS = [
        'id', 'startnode', 'endnode', 'inv_inv_biz_no', 'inv_biz_type_code',
        'odi_inv_biz_name', 'limit_type_name',	'limit01',	'limit_usd'
    ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_odi_inv_limit'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexOvscorp.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeBranchGuaranteeGuarantee.TABLE_NAME},L.odi_inv_biz_no,L.inv_biz_type_code,L.limit_type_name)) as id,
                R1.uid as startnode,
                R2.uid as endnode,
                L.inv_inv_biz_no,
                L.inv_biz_type_code,
                L.odi_inv_biz_name,
                L.limit_type_name,
                L.limit01,
                L.limit_usd
            FROM 
                (select * from {table_name} where dt = '{dt}') L
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.corp_code = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.obj_pty_code = R2.corp_code 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_odi_inv_limit',
            destination_table_name=EdgeCorpODIOvscorp.TABLE_NAME,
            destination_table_columns=EdgeCorpODIOvscorp.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeCorpPayOvscorp(GraphDataExtractTemplate):
    """
    边-7: 公司-转出-境外主体
    """

    TABLE_NAME = 'edge_corp_pay_ovscorp'
    TABLE_COLUMNS = [
        'id', 'startnode', 'endnode', 'tx_amt_usd', 'tx_code',
        'tx_ccy_code', 'tx_rem', 'settle_method_desc', 'is_taxfree_pay'
    ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_corp_pay_jn'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexOvscorp.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeBranchGuaranteeGuarantee.TABLE_NAME},L.rptno)) as id,
                R1.uid as startnode,
                R2.uid as endnode,
                L.tx_amt_usd,
                L.tx_code,
                L.tx_ccy_code,
                L.tx_rem,
                L.settle_method_desc,
                L.is_taxfree_pay
            FROM 
                (select * from {table_name} where dt = '{dt}' and tx_code != '929070' and balance_payments_type != '001') L
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.corp_code = R1.corp_code 
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.counter_party = R2.corp_code 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_pay_jn',
            destination_table_name=EdgeCorpPayOvscorp.TABLE_NAME,
            destination_table_columns=EdgeCorpPayOvscorp.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeOvscorpRcvCorp(GraphDataExtractTemplate):
    """
    边-8: 公司-转出-境外主体
    """

    TABLE_NAME = 'edge_ovscorp_rcv_corp'
    TABLE_COLUMNS = [
        'id', 'startnode', 'endnode', 'tx_amt_usd', 'tx_code',
        'tx_ccy_code', 'tx_rem', 'settle_method_desc', 'is_taxfree_pay'
    ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.pboc_corp_rcv_jn'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        table1 = f'{self.hive_biz_db_name}.{VertexOvscorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeBranchGuaranteeGuarantee.TABLE_NAME},L.rptno)) as id,
                R1.uid as startnode,
                R2.uid as endnode,
                L.tx_amt_usd,
                L.tx_code,
                L.tx_ccy_code,
                L.tx_rem,
                L.settle_method_desc,
                L.is_taxfree_pay
            FROM 
                (select * from {table_name} where dt = '{dt}' and tx_code != '929070' and balance_payments_type != '001') L
            LEFT JOIN 
                (select * from {table1}  where dt = '{self.exec_version}') R1 ON L.corp_code = R1.corp_code 
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.counter_party = R2.corp_code 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_rcv_jn',
            destination_table_name=EdgeOvscorpRcvCorp.TABLE_NAME,
            destination_table_columns=EdgeOvscorpRcvCorp.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeCorpContactPhone(GraphDataExtractTemplate):
    """
    边-9: 公司-contact-联系方式
    """

    TABLE_NAME = 'edge_corp_contact_phone'
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
        table_name5 = f'{self.hive_dw_db_name}.pboc_corp'
        dt5 = HiveUtil.newest_partition(self.spark, table_name5)
        table1 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexPhone.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeBranchGuaranteeGuarantee.TABLE_NAME},R1.uid,R2.id)) as id,
                R1.uid as startnode,
                R2.id as endnode
            FROM 
                (SELECT corp_code,filler_tel FROM {table_name1} where dt = '{dt1}'
                unionall 
                SELECT corp_code,filler_tel FROM {table_name2} where dt = '{dt2}'
                unionall 
                SELECT corp_code,operator_tel AS filler_tel FROM {table_name3} where dt = '{dt3}'
                unionall 
                SELECT corp_code,rpt_tel AS filler_tel FROM {table_name3} where dt = '{dt3}'
                unionall 
                SELECT corp_code,operator_tel AS filler_tel FROM {table_name4} where dt = '{dt4}'
                unionall 
                SELECT corp_code,rpt_tel AS filler_tel FROM {table_name4} where dt = '{dt4}'
                unionall 
                SELECT corp_code,tel AS filler_tel FROM {table_name5} where dt = '{dt5}'
                ) L
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.corp_code = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.filler_tel = R2.id 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_buy,pboc_corp_lcy,pboc_corp_pay_jn,pboc_corp_rcv_jn,pboc_corp',
            destination_table_name=EdgeCorpContactPhone.TABLE_NAME,
            destination_table_columns=EdgeCorpContactPhone.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeCorpTradeCustom(GraphDataExtractTemplate):
    """
    边-10: 公司-贸易交易-报关单
    """

    TABLE_NAME = 'edge_corp_trade_custom'
    TABLE_COLUMNS = [
        'id', 'startnode', 'endnode'
    ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name1 = f'{self.hive_dw_db_name}.pboc_expf'
        dt1 = HiveUtil.newest_partition(self.spark, table_name1)
        table_name2 = f'{self.hive_dw_db_name}.pboc_impf'
        dt2 = HiveUtil.newest_partition(self.spark, table_name2)
        table_name3 = f'{self.hive_dw_db_name}.pboc_exp_custom_rpt'
        dt3 = HiveUtil.newest_partition(self.spark, table_name3)
        table_name4 = f'{self.hive_dw_db_name}.pboc_imp_custom_rpt'
        dt4 = HiveUtil.newest_partition(self.spark, table_name4)
        table1 = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexCustom.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeBranchGuaranteeGuarantee.TABLE_NAME},R2.custom_rpt_no)) as id,
                R1.uid as startnode,
                R2.custom_rpt_no as endnode
            FROM 
                (SELECT corp_code,custom_rpt_no FROM {table_name1} where dt = '{dt1}'
                unionall 
                SELECT corp_code,custom_rpt_no FROM {table_name2} where dt = '{dt2}'
                unionall 
                SELECT corp_code,custom_rpt_no FROM {table_name3} where dt = '{dt3}'
                unionall 
                SELECT corp_code,custom_rpt_no FROM {table_name4} where dt = '{dt4}'
                ) L
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.corp_code = R1.corp_code
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.custom_rpt_no = R2.custom_rpt_no 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_expf,pboc_impf,pboc_exp_custom_rpt,pboc_imp_custom_rpt',
            destination_table_name=EdgeCorpTradeCustom.TABLE_NAME,
            destination_table_columns=EdgeCorpTradeCustom.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )



class EdgeCustomRegisterMerch(GraphDataExtractTemplate):
    """
    边-11: 报关单-登记-商品
    """

    TABLE_NAME = 'edge_custom_register_merch'
    TABLE_COLUMNS = [
        'id', 'startnode', 'endnode', 'prod_deal_amt_usd'
    ]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name1 = f'{self.hive_dw_db_name}.pboc_expf'
        dt1 = HiveUtil.newest_partition(self.spark, table_name1)
        table_name2 = f'{self.hive_dw_db_name}.pboc_impf'
        dt2 = HiveUtil.newest_partition(self.spark, table_name2)
        table1 = f'{self.hive_biz_db_name}.{VertexCustom.TABLE_NAME}'
        table2 = f'{self.hive_biz_db_name}.{VertexMerch.TABLE_NAME}'

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeBranchGuaranteeGuarantee.TABLE_NAME},R1.custom_rpt_no,R2.uid)) as id,
                R1.custom_rpt_no as startnode,
                R2.uid as endnode,
                L.prod_deal_amt_usd
            FROM 
                (SELECT substring(merch_code,0,1) as merch_code,custom_rpt_no,prod_deal_amt_usd FROM {table_name1} where dt = '{dt1}'
                unionall 
                SELECT substring(merch_code,0,1) as merch_code,custom_rpt_no,prod_deal_amt_usd FROM {table_name2} where dt = '{dt2}'
                ) L
            LEFT JOIN 
                (select * from {table1} where dt = '{self.exec_version}') R1 ON L.custom_rpt_no = R1.custom_rpt_no
            LEFT JOIN 
                (select * from {table2} where dt = '{self.exec_version}') R2 ON L.merch_code = R2.uid 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_expf,pboc_impf',
            destination_table_name=EdgeCustomRegisterMerch.TABLE_NAME,
            destination_table_columns=EdgeCustomRegisterMerch.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )
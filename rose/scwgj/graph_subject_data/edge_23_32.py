#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: edge_23_32
:Author: zhouchao@bbdservice.com
:Date: 2021-05-19 3:53 PM
:Version: v.1.0
:Description:
"""
from pyspark.sql import DataFrame

from scwgj.graph_subject_data.extract_core import GraphDataExtractTemplate, TableMetaData
from scwgj.graph_subject_data.node_1_6 import VertexCorp, VertexPerson
from scwgj.proj_common.hive_util import HiveUtil


class EdgeCorpTradeCustom(GraphDataExtractTemplate):
    """
    边-23: 公司-贸易交易-报关单
    """

    TABLE_NAME = 'edge_corp_trade_custom'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.off_line_relations'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        company_table = f'{self.hive_biz_db_name}.{VertexPerson.TABLE_NAME}'
        finical_table = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'  # TODO: add finical tale name

        df = self.spark.sql(f"""
            SELECT 
                md5(concat_ws('-', {EdgeCorpTradeCustom.TABLE_NAME}, L.source_bbd_id, L.destination_bbd_id)) as id,
                R1.ID as startnode,
                R2.uid as endnode
            FROM 
                (   SELECT * 
                    FROM {table_name} 
                    WHERE source_degree<=2 
                    AND destination_degree<=2 
                    AND relation_type=DIRECTOR 
                    AND source_isperson=1 
                    AND destination_isperson=0
                    AND dt='{dt}'
                ) L 
            LEFT JOIN 
                (select * from {company_table} where dt = '{self.exec_version}') R1 ON L.source_bbd_id = R1.ID
            LEFT JOIN 
                (select * from {finical_table} where dt = '{self.exec_version}') R2 ON L.destination_bbd_id = R2.bbd_qyxx_id 
        """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='',
            destination_table_name=EdgeCorpTradeCustom.TABLE_NAME,
            destination_table_columns=EdgeCorpTradeCustom.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeCustomRegisterMerch(GraphDataExtractTemplate):
    """
    边-24: 报关单-登记-商品
    """

    TABLE_NAME = 'edge_custom_register_merch'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.off_line_relations'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        company_table = f'{self.hive_biz_db_name}.{VertexPerson.TABLE_NAME}'
        finical_table = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'  # TODO: add finical tale name

        df = self.spark.sql(f"""
                SELECT 
                    md5(concat_ws('-', {EdgeCustomRegisterMerch.TABLE_NAME}, 
                                        L.source_bbd_id, L.destination_bbd_id)) as id,
                    R1.ID as startnode,
                    R2.uid as endnode
                FROM 
                    (   SELECT * 
                        FROM {table_name} 
                        WHERE source_degree<=2 
                        AND destination_degree<=2 
                        AND relation_type=SUPERVISOR 
                        AND source_isperson=1 
                        AND destination_isperson=0
                        AND dt='{dt}'
                    ) L 
                LEFT JOIN 
                    (select * from {company_table} where dt = '{self.exec_version}') R1 ON L.source_bbd_id = R1.ID
                LEFT JOIN 
                    (select * from {finical_table} where dt = '{self.exec_version}') R2 ON L.destination_bbd_id = R2.bbd_qyxx_id 
            """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='',
            destination_table_name=EdgeCustomRegisterMerch.TABLE_NAME,
            destination_table_columns=EdgeCustomRegisterMerch.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeCorpHasFxaccount(GraphDataExtractTemplate):
    """
    边-25: 公司-拥有-外汇账户
    """

    TABLE_NAME = 'edge_corp_has_fxaccount'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode', "invest_ratio", "invest_amount"]

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.off_line_relations'
        table_gd = f'{self.hive_dw_db_name}.qyxx_gdxx'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        gd_dt = HiveUtil.newest_partition(self.spark, table_gd)
        company_table = f'{self.hive_biz_db_name}.{VertexPerson.TABLE_NAME}'
        finical_table = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'  # TODO: add finical tale name

        df = self.spark.sql(f"""
                SELECT 
                    md5(concat_ws('-', {EdgeCorpHasFxaccount.TABLE_NAME}, L.source_bbd_id, L.destination_bbd_id)) as id,
                    R1.ID as startnode,
                    R2.uid as endnode,
                    R3.invest_ratio,
                    R3.amount
                FROM 
                    (   SELECT * 
                        FROM {table_name} 
                        WHERE source_degree<=2 
                        AND destination_degree<=2 
                        AND relation_type=INVEST 
                        AND source_isperson=1 
                        AND destination_isperson=0
                        AND dt='{dt}'
                    ) L 
                LEFT JOIN 
                    (select * from {company_table} where dt = '{self.exec_version}') R1 ON L.source_bbd_id = R1.ID
                LEFT JOIN 
                    (select * from {finical_table} where dt = '{self.exec_version}') R2 ON L.destination_bbd_id = R2.bbd_qyxx_id 
                LEFT JOIN 
                    (SELECT * 
                     FROM {table_gd} 
                     WHERE name_compid=1 
                     AND dt = '{gd_dt}'
                    ) R3 ON R1.ID = R3.shareholder_id AND R2.bbd_qyxx_id = R3.bbd_qyxx_id
            """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='',
            destination_table_name=EdgeCorpHasFxaccount.TABLE_NAME,
            destination_table_columns=EdgeCorpHasFxaccount.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )


class EdgeCorpHasRmbaccount(GraphDataExtractTemplate):
    """
    边-26: 公司-拥有-人民币账户
    """

    TABLE_NAME = 'edge_corp_has_rmbaccount'
    TABLE_COLUMNS = ['id', 'startnode', 'endnode']

    def extract_data(self, df: DataFrame = None) -> DataFrame:
        table_name = f'{self.hive_dw_db_name}.off_line_relations'
        dt = HiveUtil.newest_partition(self.spark, table_name)
        company_table = f'{self.hive_biz_db_name}.{VertexPerson.TABLE_NAME}'
        finical_table = f'{self.hive_biz_db_name}.{VertexCorp.TABLE_NAME}'  # TODO: add finical tale name

        df = self.spark.sql(f"""
                    SELECT 
                        md5(concat_ws('-', {EdgeCorpHasRmbaccount.TABLE_NAME}, 
                                        L.source_bbd_id, L.destination_bbd_id)) as id,
                        R1.ID as startnode,
                        R2.uid as endnode
                    FROM 
                        (SELECT * 
                            FROM {table_name} 
                            WHERE source_degree<=2 
                            AND destination_degree<=2 
                            AND relation_type=LEGAL 
                            AND source_isperson=1 
                            AND destination_isperson=0
                            AND dt='{dt}'
                        ) L 
                    LEFT JOIN 
                        (select * from {company_table} where dt = '{self.exec_version}') R1 ON L.source_bbd_id = R1.ID
                    LEFT JOIN 
                        (select 8 from {finical_table} where dt = '{self.exec_version}') R2 ON L.destination_bbd_id = R2.bbd_qyxx_id 
                """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='',
            destination_table_name=EdgeCorpHasRmbaccount.TABLE_NAME,
            destination_table_columns=EdgeCorpHasRmbaccount.TABLE_COLUMNS,
            primary_keys=['id'],
            has_update_data=False
        )

#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: node.py
:Author: xufeng@bbdservice.com 
:Date: 2021-04-29 2:50 PM
:Version: v.1.0
:Description:
"""
from pyspark.sql import DataFrame, Row

from scwgj.graph_subject_data.extract_core import GraphDataExtractTemplate, TableMetaData
from scwgj.proj_common.hive_util import HiveUtil


class VertexCorp(GraphDataExtractTemplate):
    """
    实体表－1:　公司(境内主体)
    """

    TABLE_NAME = 'vertex_corp'

    TABLE_COLUMNS = ['uid', 'bbd_qyxx_id', 'corp_code', 'company_name', 'safecode', 'region_code',
                     'indu_attr_code', 'indu_attr_desc', 'company_type', 'esdate', 'address', 'regcap_amt_usd',
                     'fcy_regcap_amt_usd','inv_amt_usd', 'fcy_inv_amt_usd', 'country_code',
                     'enterprise_status', 'risk_black', 'black_reason']

    def extract_data(self) -> DataFrame:
        table_name1 = f'{self.hive_dw_db_name}.pboc_corp'
        dt1 = HiveUtil.newest_partition(self.spark, table_name1)
        table_name2 = f'{self.hive_dw_db_name}.pboc_guar_sign'
        dt2 = HiveUtil.newest_partition(self.spark, table_name2)
        table_name3 = f'{self.hive_dw_db_name}.pboc_dfxloan_sign'
        dt3 = HiveUtil.newest_partition(self.spark, table_name3)
        table_name4 = f'{self.hive_dw_db_name}.pboc_fdi_sh'
        dt4 = HiveUtil.newest_partition(self.spark, table_name4)
        table_name5 = f'{self.hive_dw_db_name}.pboc_odi_inv_limit'
        dt5 = HiveUtil.newest_partition(self.spark, table_name5)
        table_name6 = f'{self.hive_dw_db_name}.pboc_frfdi_conf'
        dt6 = HiveUtil.newest_partition(self.spark, table_name6)
        table_name7 = f'{self.hive_dw_db_name}.pboc_frfdi_sta'
        dt7 = HiveUtil.newest_partition(self.spark, table_name7)
        table_name8 = f'{self.hive_dw_db_name}.pboc_acctno'
        dt8 = HiveUtil.newest_partition(self.spark, table_name8)
        table_name9 = f'{self.hive_dw_db_name}.pboc_corp_buy'
        dt9 = HiveUtil.newest_partition(self.spark, table_name9)
        table_name10 = f'{self.hive_dw_db_name}.pboc_corp_lcy'
        dt10 = HiveUtil.newest_partition(self.spark, table_name10)
        table_name11 = f'{self.hive_dw_db_name}.pboc_corp_pay_jn'
        dt11= HiveUtil.newest_partition(self.spark, table_name11)
        table_name12 = f'{self.hive_dw_db_name}.pboc_corp_rcv_jn'
        dt12= HiveUtil.newest_partition(self.spark, table_name12)
        table_name13 = f'{self.hive_dw_db_name}.pboc_expf'
        dt13= HiveUtil.newest_partition(self.spark, table_name13)
        table_name14 = f'{self.hive_dw_db_name}.pboc_impf'
        dt14= HiveUtil.newest_partition(self.spark, table_name14)
        table_name15 = f'{self.hive_dw_db_name}.pboc_exp_custom_rpt'
        dt15= HiveUtil.newest_partition(self.spark, table_name15)
        table_name16 = f'{self.hive_dw_db_name}.pboc_imp_custom_rpt'
        dt16= HiveUtil.newest_partition(self.spark, table_name16)
        table_name17 = f'{self.hive_dw_db_name}.t_acct_rpb'
        dt17 = HiveUtil.newest_partition(self.spark, table_name17)
        table_name18 = f'{self.hive_dw_db_name}.t_acct_pay_dtl'
        dt18 = HiveUtil.newest_partition(self.spark, table_name18)
        table_name19 = f'{self.hive_dw_db_name}.t_acct_rcv_dtl'
        dt19 = HiveUtil.newest_partition(self.spark, table_name19)
        table_name20 = f'{self.hive_dw_db_name}.t_acct_pay_dtl'
        dt20 = HiveUtil.newest_partition(self.spark, table_name20)
        table_name21 = f'{self.hive_dw_db_name}.t_acct_rcv_dtl'
        dt21 = HiveUtil.newest_partition(self.spark, table_name21)

        table_basic = f'{self.hive_dw_db_name}.qyxx_basic'
        dt_basic = HiveUtil.newest_partition(self.spark, table_basic)

        table_black = f'{self.hive_dw_db_name}.risk_black'
        table_black_dt = HiveUtil.newest_partition(self.spark, table_black)

        self.spark.sql(f"""
                select bbd_qyxx_id, 
                       concat_ws(',',collect_set(reason)) as black_reason
                from (
                    select bbd_qyxx_id,
                           case when property=1001 then '买卖违约'
                                when property=1002 then '借贷违约'
                                when property=1003 then '失信被执行人'
                                when property=1004 then '被执行人'
                                when property=1005 then '税务违法'
                                when property=1006 then '经营异常'
                           end reason
                    where dt = '{table_black_dt}'
                    and property in (1001, 1002, 1003, 1004, 1005, 1006)
                    )
                group by bbd_qyxx_id
            """).createOrReplaceTempView('black_tmp')

        table_1_19_df = self.spark.sql(f"""
            SELECT  corp_code as uid, 
                        bbd_qyxx_id, corp_code, pty_name as company_name, 
                        safecode, region_code, indu_attr_code, 
                        indu_attr_desc, '' company_type, '' esdate,
                        addr_desc as address, regcap_amt_usd, fcy_regcap_amt_usd, 
                        inv_amt_usd, fcy_inv_amt_usd, country_code, 
                        '' enterprise_status, '' risk_black, '' black_reason,
                        1 as rank
                FROM {table_name1} 
                where dt = '{dt1}' and econ_type_code!='400' 
                and (source_key = 'pty_name' or source_key = 'corp_code' or source_key = 'license_no')
            UNION
                SELECT  app_code as uid,
                        bbd_qyxx_id, app_code as corp_code, app_name as company_name,
                        '' safecode, '' region_code, '' indu_attr_code,
                        '' indu_attr_desc, '' company_type, '' esdate,
                        '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                        '' enterprise_status, '' risk_black, '' black_reason,
                        2 as rank
                FROM {table_name2} where dt = '{dt2}' 
                AND (source_key = 'app_code' or source_key = 'app_name')
            union
                SELECT  corp_code as uid,
                        bbd_qyxx_id, corp_code, corp_name as company_name,
                        safecode, '' region_code, indu_attr_code,
                        indu_attr_desc, '' company_type, '' esdate,
                        '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                        '' enterprise_status, '' risk_black, '' black_reason,
                        3 as rank
                FROM {table_name3} where dt = '{dt3}' 
                and (source_key = 'corp_code' or source_key = 'corp_name')
            union
                SELECT  pty_code as uid,
                        bbd_qyxx_id, pty_code as corp_code, pty_name as company_name,
                        safecode, '' region_code, indu_attr_code,
                        indu_attr_desc, '' company_type, '' esdate,
                        '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                        '' enterprise_status, '' risk_black, '' black_reason,
                        4 as rank
                FROM {table_name4} where dt = '{dt4}' 
                and (source_key = 'pty_code' or source_key = 'pty_name')
            union
                SELECT  corp_code as uid,
                        bbd_qyxx_id, corp_code, corp_name as company_name,
                        safecode, '' region_code, indu_attr_code,
                        indu_attr_desc, '' company_type, '' esdate,
                        '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                        '' enterprise_status, '' risk_black, '' black_reason,
                        5 as rank
                FROM {table_name5} where dt = '{dt5}' 
                and (source_key = 'corp_code' or source_key = 'pty_name')
            union
                SELECT  pty_code as uid,
                        bbd_qyxx_id, pty_code as corp_code, pty_name as company_name,
                        safecode, '' region_code, indu_attr_code, indu_attr_desc,
                        '' company_type, '' esdate, '' address, '' regcap_amt_usd,
                        '' fcy_regcap_amt_usd, '' inv_amt_usd, '' fcy_inv_amt_usd,
                        '' country_code, '' enterprise_status, '' risk_black, '' black_reason,
                        6 as rank
                FROM {table_name6} where dt = '{dt6}' 
                and (source_key = 'pty_code' or source_key = 'pty_name')
            union
                SELECT  pty_code as uid,
                        bbd_qyxx_id, pty_code as corp_code, pty_name as company_name,
                        safecode, '' region_code, indu_attr_code, indu_attr_desc,
                        '' company_type, '' esdate, '' address, '' regcap_amt_usd,
                        '' fcy_regcap_amt_usd, '' inv_amt_usd, '' fcy_inv_amt_usd,
                        '' country_code, '' enterprise_status, '' risk_black, '' black_reason,
                        7 as rank
                FROM {table_name7} where dt = '{dt7}' 
                and (source_key = 'pty_code' or source_key = 'pty_name')
            union
                SELECT  acctno_pty_code as uid,
                        bbd_qyxx_id,acctno_pty_code as corp_code,
                        acctno_pty_name as company_name,
                        '' safecode,'' region_code,
                        '' indu_attr_code,'' indu_attr_desc,
                        '' company_type,'' esdate,'' address,
                        '' regcap_amt_usd,'' fcy_regcap_amt_usd,
                        '' inv_amt_usd,'' fcy_inv_amt_usd,
                        '' country_code,'' enterprise_status,
                        '' risk_black,'' black_reason, 8 as rank
                FROM {table_name8} where dt = '{dt8}' and econ_type_code!='400' 
                and  (source_key = 'acctno_pty_code' or source_key = 'acctno_pty_name')
            union
                SELECT  corp_code as uid,
                        bbd_qyxx_id,corp_code,
                        pty_name2 as company_name,
                        safecode,'' region_code,
                        indu_attr_code,indu_attr_desc,
                        '' company_type,'' esdate,'' address,
                        '' regcap_amt_usd,'' fcy_regcap_amt_usd,
                        '' inv_amt_usd,'' fcy_inv_amt_usd,
                        '' country_code,'' enterprise_status,
                        '' risk_black,'' black_reason, 9 as rank
                FROM {table_name9} where dt = '{dt9}' and econ_type_code!='400' and 
                and (source_key = 'corp_code' or source_key = 'pty_name2')
            union
                SELECT  corp_code as uid,
                        bbd_qyxx_id,corp_code,
                        pty_name as company_name,safecode,'' region_code,
                        indu_attr_code,indu_attr_desc,
                        '' company_type,'' esdate,'' address,
                        '' regcap_amt_usd,'' fcy_regcap_amt_usd,
                        '' inv_amt_usd,'' fcy_inv_amt_usd,
                        '' country_code,'' enterprise_status,
                        '' risk_black,'' black_reason, 10 as rank
                FROM {table_name10} where dt = '{dt10}' and econ_type_code!='400' 
                and (source_key = 'corp_code' or source_key = 'pty_name')
            union
                SELECT  corp_code as uid,
                        bbd_qyxx_id,corp_code,
                        pty_name1 as company_name,safecode,'' region_code,
                        indu_attr_code,indu_attr_desc,
                        '' company_type,'' esdate,'' address,
                        '' regcap_amt_usd,'' fcy_regcap_amt_usd,
                        '' inv_amt_usd,'' fcy_inv_amt_usd,
                        '' country_code,'' enterprise_status,'
                        ' risk_black,'' black_reason, 11 as rank
                FROM {table_name11} where dt = '{dt11}' and tx_code!= '822030' 
                and (source_key = 'corp_code' or source_key = 'pty_name1')
            union
                SELECT  corp_code as uid,
                        bbd_qyxx_id, corp_code, pty_name as company_name,
                        safecode, '' region_code, indu_attr_code,
                        indu_attr_desc, '' company_type, '' esdate,
                        '' address,'' regcap_amt_usd,'' fcy_regcap_amt_usd,
                        '' inv_amt_usd,'' fcy_inv_amt_usd,'' country_code,
                        '' enterprise_status,'' risk_black,'' black_reason, 12 as rank
                FROM {table_name12} where dt = '{dt12}' and tx_code!= '822030' 
                and (source_key = 'corp_code' or source_key = 'pty_name')
            union
                SELECT  corp_code as uid,
                        bbd_qyxx_id,corp_code,corp_name as company_name,
                        safecode,'' region_code,indu_attr_code,indu_attr_desc,
                        '' company_type,'' esdate,'' address,'' regcap_amt_usd,
                        '' fcy_regcap_amt_usd,'' inv_amt_usd,'' fcy_inv_amt_usd,
                        '' country_code,'' enterprise_status,'' risk_black,'' black_reason, 13 as rank
                FROM {table_name13} where dt = '{dt13}' 
                and (source_key = 'corp_code' or source_key = 'corp_name')
            union
                SELECT  corp_code as uid,
                        bbd_qyxx_id, corp_code, corp_name as company_name, safecode,
                        '' region_code, indu_attr_code, indu_attr_desc, '' company_type,
                        '' esdate, '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code, '' enterprise_status,
                        '' risk_black, '' black_reason, 14 as rank
                FROM {table_name14} where dt = '{dt14}' 
                and (source_key = 'corp_code' or source_key = 'corp_name')
            union
                SELECT  corp_code as uid,
                        bbd_qyxx_id,corp_code, corp_name as company_name,
                        safecode, '' region_code, indu_attr_code, indu_attr_desc,
                        '' company_type, '' esdate,'' address, '' regcap_amt_usd,
                        '' fcy_regcap_amt_usd, '' inv_amt_usd, '' fcy_inv_amt_usd,
                        '' country_code, '' enterprise_status, '' risk_black, '' black_reason,
                        15 as rank
                FROM {table_name15} where dt = '{dt15}' 
                and (source_key = 'corp_code' or source_key = 'corp_name')
            union
                SELECT  corp_code as uid,
                        bbd_qyxx_id, corp_code, corp_name as company_name,
                        safecode, '' region_code, indu_attr_code,
                        indu_attr_desc, '' company_type, '' esdate,
                        '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                        '' enterprise_status, '' risk_black, '' black_reason,
                        16 as rank
                FROM {table_name16} where dt = '{dt16}' 
                and (source_key = 'corp_code' or source_key = 'corp_name') 
            union 
                SELECT  corp_code as uid,
                        bbd_qyxx_id, corp_code, pty_name as company_name,
                        safecode, '' region_code, indu_attr_code,
                        indu_attr_desc, '' company_type, '' esdate,
                        '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                        '' enterprise_status, '' risk_black, '' black_reason,
                        17 as rank
                FROM {table_name17} where dt = '{dt17}' 
                and source_key = 'corp_code' 
            union 
                SELECT  payer_code as uid,
                        bbd_qyxx_id, payer_code as corp_code, cp_payer_name as company_name,
                        safecode2 as safecode, '' region_code, indu_attr_code,
                        indu_attr_desc, '' company_type, '' esdate,
                        '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                        '' enterprise_status, '' risk_black, '' black_reason,
                        18 as rank
                FROM {table_name18} where dt = '{dt18}' 
                and source_key = 'payer_code' 
            union 
                SELECT  rcver_code as uid,
                        bbd_qyxx_id, rcver_code as corp_code, acctno_pty_name as company_name,
                        safecode2 as safecode, '' region_code, indu_attr_code,
                        indu_attr_desc, '' company_type, '' esdate,
                        '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                        '' enterprise_status, '' risk_black, '' black_reason,
                        19 as rank
                FROM {table_name19} where dt = '{dt19}' 
                and source_key = 'rcver_code'  
        """)
        table_1_19_df.union(table_20_df).union(table_21_df).createOrReplaceTempView('basic_tmp')

        table_20_df = self.spark.sql(f"""
                select b.acctno_pty_code as uid,
                        b.bbd_qyxx_id, b.acctno_pty_code as corp_code, a.payer_name as company_name,
                        '' safecode, '' region_code, a.indu_attr_code,
                        a.indu_attr_desc, '' company_type,'' esdate,
                        '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                        '' enterprise_status, '' risk_black, '' black_reason,
                        20 as rank
                from (select * 
                      from {table_name20}
                      where dt = '{dt20}') a
                join (select * 
                      from {table_name8} 
                      where dt = '{dt8}' 
                      and  (source_key = 'acctno_pty_code' or source_key = 'acctno_pty_name')) b
                on a.payer_acctno = b.acctno
                union 
                select d.corp_code as uid,
                        d.bbd_qyxx_id, d.corp_code as corp_code, c.payer_name as company_name,
                        '' safecode, '' region_code, c.indu_attr_code,
                        c.indu_attr_desc, '' company_type,'' esdate,
                        '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                        '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                        '' enterprise_status, '' risk_black, '' black_reason,
                        20 as rank
                from (select * 
                      from {table_name20}
                      where dt = '{dt20}') c
                join (select * 
                      from {table_name1} 
                      where dt = '{dt1}' 
                      and  source_key = 'rcver_code') d
                on c.payer_name = d.pty_name
            """)

        table_21_df = self.spark.sql(f"""
                        select b.acctno_pty_code as uid,
                                b.bbd_qyxx_id, b.acctno_pty_code as corp_code, a.rcver_name as company_name,
                                '' safecode, '' region_code, a.indu_attr_code,
                                a.indu_attr_desc, '' company_type,'' esdate,
                                '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                                '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                                '' enterprise_status, '' risk_black, '' black_reason,
                                20 as rank
                        from (select * 
                              from {table_name21}
                              where dt = '{dt21}') a
                        join (select * 
                              from {table_name8} 
                              where dt = '{dt8}' 
                              and  (source_key = 'acctno_pty_code' or source_key = 'acctno_pty_name')) b
                        on a.rcver_acctno = b.acctno
                        union 
                        select d.corp_code as uid,
                                d.bbd_qyxx_id, d.corp_code as corp_code, c.rcver_name as company_name,
                                '' safecode, '' region_code, c.indu_attr_code,
                                c.indu_attr_desc, '' company_type,'' esdate,
                                '' address, '' regcap_amt_usd, '' fcy_regcap_amt_usd,
                                '' inv_amt_usd, '' fcy_inv_amt_usd, '' country_code,
                                '' enterprise_status, '' risk_black, '' black_reason,
                                21 as rank
                        from (select * 
                              from {table_name21}
                              where dt = '{dt21}') c
                        join (select * 
                              from {table_name1} 
                              where dt = '{dt1}' 
                              and  source_key = 'rcver_code') d
                        on c.rcver_name = d.pty_name
                    """)

        table_1_19_df.union(table_20_df).union(table_21_df).createOrReplaceTempView('basic_tmp')
        self.spark.sql("""
                    select  uid,
                            bbd_qyxx_id, corp_code, company_name,
                            safecode, region_code, indu_attr_code,
                            indu_attr_desc, company_type, esdate,
                            address, regcap_amt_usd, fcy_regcap_amt_usd,
                            inv_amt_usd, fcy_inv_amt_usd, country_code,
                            enterprise_status, risk_black, black_reason
                    from (
                        select *, row_number() over(partition by uid order by rank) as rn 
                        from basic_tmp
                        )
                    where rn=1
                    """).createOrReplaceTempView('result_tmp')

        df = self.spark.sql(f"""
                select  uid, bbd_qyxx_id, corp_code, company_name,
                        safecode, region_code, indu_attr_code,
                        indu_attr_desc, company_type, esdate,
                        address, regcap_amt_usd, fcy_regcap_amt_usd,
                        inv_amt_usd, fcy_inv_amt_usd, country_code,
                        enterprise_status, black_reason,
                    case when black_reason rlke '违约|违法|执行人|异常' then '是'
                         else '否'
                         risk_black
                    (select a.uid,
                        a.bbd_qyxx_id, a.corp_code, a.company_name,
                        a.safecode, a.region_code, a.indu_attr_code,
                        a.indu_attr_desc, b.company_type, b.esdate,
                        b.address, a.regcap_amt_usd, a.fcy_regcap_amt_usd,
                        a.inv_amt_usd, a.fcy_inv_amt_usd, a.country_code,
                        b.enterprise_status, c.risk_black, c.black_reason,
                        risk_black
                    from result_tmp
                    left join (select * from {table_basic} where dt = '{dt_basic}') a
                    on a.bbd_qyxx_id = b.bbd_qyxx_id
                    left join black_tmp c
                    on a.bbd_qyxx_id = c.bbd_qyxx_id)
            """)
        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='',
            destination_table_name=VertexCorp.TABLE_NAME,
            destination_table_columns=VertexCorp.TABLE_COLUMNS,
            primary_keys=['UID'],
        )


class VertexOvscorp(GraphDataExtractTemplate):
    """
    实体表－2:　公司(境外主体)
    """

    TABLE_NAME = 'vertex_ovscorp'

    TABLE_COLUMNS = ['UID', 'pty_name', 'pty_enname', 'reg_country_code', 'is_odi']

    def extract_data(self) -> DataFrame:
        table_name1 = f'{self.hive_dw_db_name}.pboc_ovs_corp'
        dt1 = HiveUtil.newest_partition(self.spark, table_name1)
        table_name2 = f'{self.hive_dw_db_name}.pboc_fdi_sh'
        dt2 = HiveUtil.newest_partition(self.spark, table_name2)
        table_name3 = f'{self.hive_dw_db_name}.pboc_odi_inv_limit'
        dt3 = HiveUtil.newest_partition(self.spark, table_name3)
        table_name4 = f'{self.hive_dw_db_name}.pboc_corp_pay_jn'
        dt4 = HiveUtil.newest_partition(self.spark, table_name4)
        table_name5 = f'{self.hive_dw_db_name}.pboc_corp_rcv_jn'
        dt5 = HiveUtil.newest_partition(self.spark, table_name5)
        table_name6 = f'{self.hive_dw_db_name}.pboc_guar_sign'
        dt6 = HiveUtil.newest_partition(self.spark, table_name6)
        table_name7 = f'{self.hive_dw_db_name}.pboc_corp'
        dt7= HiveUtil.newest_partition(self.spark, table_name7)
        table_name8 = f'{self.hive_dw_db_name}.pboc_acctno'
        dt8= HiveUtil.newest_partition(self.spark, table_name8)

        table_1_8_df = self.spark.sql(f"""
            SELECT pty_code as UID, pty_name, pty_enname,
                   reg_country_code, is_odi, 1 as rank 
            FROM {table_name1} 
            where dt = '{dt1}'
            union
            SELECT sh_pty_code as UID, '' pty_name, '' pty_enname,
                   '' reg_country_code, '' is_odi, 2 as rank 
            FROM {table_name2} 
            where dt = '{dt2}'
            union
            SELECT obj_pty_code as UID, '' pty_name, '' pty_enname,
                   '' reg_country_code, '' is_odi, 3 as rank  
            FROM {table_name3} where dt = '{dt3}' 
            union
            SELECT guartee_code as UID, guartee_name as pty_name, guatee_enname as pty_enname,
                   benef_countrt_code as reg_country_code, '' is_odi, 4 as rank 
            FROM {table_name6} 
            where dt = '{dt6}'
            and country_code!=CHN
            union
            SELECT pboc_corp as UID, pty_enname as pty_name, pty_name as pty_enname,
                   '' reg_country_code, '' is_odi, 5 as rank 
            FROM {table_name7} 
            where dt = '{dt7}'
            and econ_type_code=400 
            union
            SELECT acctno_pty_code as UID, '' pty_name, acctno_pty_name as pty_enname,
                   '' reg_country_code, '' is_odi, 6 as rank 
            FROM {table_name8} 
            where dt = '{dt8}'
            and econ_type_code=400
        """)
        table_1_8_df.createOrReplaceTempView('basic_tmp')
        table_4_df = self.spark.sql(f"""
            select md5(concat_ws('-', {VertexOvscorp.TABLE_NAME}, a.counter_party) as UID, a.counter_party as pty_name, 
                   a.counter_party as pty_enname, a.rcver_country_code as reg_country_code, b.is_odi, 7 as rank
            from (select * from {table_name4} where dt = '{dt4}' and balance_payments_type!=001) a
            left join basic_tmp b
            on a.counter_party = b.pty_enname
            """).where('is_odi is null')
        table_5_df = self.spark.sql(f"""
                    select md5(concat_ws('-', {VertexOvscorp.TABLE_NAME}, a.counter_party) as UID, a.counter_party as pty_name, 
                           a.counter_party as pty_enname, a.payer_country_code as reg_country_code, b.is_odi, 7 as rank
                    from (select * from {table_name5} where dt = '{dt5}' and balance_payments_type!=001) a
                    left join basic_tmp b
                    on a.counter_party = b.pty_enname
                    """).where('is_odi is null')
        table_1_8_df.union(table_4_df).union(table_5_df).createOrReplaceTempView('result_tmp')
        df = self.spark.sql(f"""
            select {','.join(VertexOvscorp.TABLE_COLUMNS)}
            from (
                select *, row_number over(partition by UID order by rank) rn
                from result_tmp
                ) 
            where rn=1
            """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_ovs_corp,pboc_corp_pay_jn,pboc_corp_rcv_jn,pboc_guar_sign,pboc_corp,pboc_acctno',
            destination_table_name=VertexOvscorp.TABLE_NAME,
            destination_table_columns=VertexOvscorp.TABLE_COLUMNS,
            primary_keys=['UID'],
        )



class VertexPerson(GraphDataExtractTemplate):
    """
    实体表－3:　自然人
    """

    TABLE_NAME = 'vertex_person'

    TABLE_COLUMNS = [
        'ID', 'person_name', 'person_sex',  'person_cerno', 'person_phonenumber', 'address', 'company'
    ]

    def extract_data(self) -> DataFrame:
        table_name1 = f'{self.hive_dw_db_name}.off_line_relations'
        dt1 = HiveUtil.newest_partition(self.spark, table_name1)
        table_name2 = f'{self.hive_dw_db_name}.pboc_corp'
        dt2 = HiveUtil.newest_partition(self.spark, table_name2)
        table_name3 = f'{self.hive_dw_db_name}.qyxx_gdxx'
        dt3 = HiveUtil.newest_partition(self.spark, table_name3)
        table_name4 = f'{self.hive_dw_db_name}.qyxx_basic'
        dt4 = HiveUtil.newest_partition(self.spark, table_name4)
        table_name5 = f'{self.hive_dw_db_name}.qyxx_baxx'
        dt5 = HiveUtil.newest_partition(self.spark, table_name5)
        table_name6 = f'{self.hive_dw_db_name}.shareholder'
        dt6 = HiveUtil.newest_partition(self.spark, table_name6)

        tmp_df = self.spark.sql(f"""
            SELECT 
                ID,person_name,person_sex,person_cerno,person_phonenumber,address,company
            FROM 
                (SELECT source_bbd_id as ID, bbd_qyxx_id, source_name as person_name,'' person_sex,'' person_cerno,'' person_phonenumber,
                '' person_phonenumber,'' company 
                FROM {table_name1} 
                WHERE source_degree<=2 
                AND destination_degree<=2 
                AND source_isperson=1 
                AND dt = '{dt1}'
            union
                SELECT destination_bbd_id as ID, bbd_qyxx_id, destination_name as person_name,'' person_sex,'' person_cerno, 
                       '' person_phonenumber,'' company 
                FROM {table_name1} 
                WHERE source_degree<=2 
                AND destination_degree<=2 
                AND destination_isperson=1
                AND dt = '{dt1}'
            union
                SELECT bbd_qyxx_id as ID, bbd_qyxx_id, frname as person_name, '' person_sex,'' person_cerno,
                       '' person_phonenumber,'' person_phonenumber,'' company 
                FROM {table_name2} 
                where dt = '{dt2}'
                and frname is not null
                
            union
                SELECT shareholder_id as ID, bbd_qyxx_id, shareholder_name as person_name,'' as person_sex, 
                       cerno as person_cerno,'' person_phonenumber,
                       '' person_phonenumber,'' company 
                FROM {table_name6} 
                where dt = '{dt6}'
            union
                SELECT frname_id as ID, bbd_qyxx_id, frname as person_name,'' as person_sex, frname_cerno as person_cerno,
                       '' person_phonenumber,'' person_phonenumber,'' company 
                FROM {table_name4} 
                where dt = '{dt4}'
        """)
        tmp_df.drop_duplicates(['bbd_qyxx_id', 'person_name']).createOrReplaceTempView('result_tmp')

        df = self.spark.sql(f"""
                select a.ID, a.person_name, b.person_sex, 
                        case when b.cerno is not null then  b.cerno
                             when c.cerno is not null then  c.cerno
                             when d.frname_cerno is not null then  d.frname_cerno
                        else a.person_cerno
                        end person_cerno
                       a.person_phonenumber, a.person_phonenumber, a.company 
                from result_tmp
                left join (select * from {table_name5} where dt = '{dt5}') b
                on a.ID = b.name_id
                left join (select * from {table_name3} where dt = '{dt3}') c
                on a.ID = c.shareholder_id
                left join (select * from {table_name4} where dt = '{dt4}') d
                on a.ID = d.frname_id
            """)

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='off_line_relations,pboc_corp,qyxx_gdxx,qyxx_basic,qyxx_baxx',
            destination_table_name=VertexPerson.TABLE_NAME,
            destination_table_columns=VertexPerson.TABLE_COLUMNS,
            primary_keys=['ID'],
        )




class VertexFxaccount(GraphDataExtractTemplate):
    """
    实体表－4:　外汇账户
    """

    TABLE_NAME = 'vertex_fxaccount'

    TABLE_COLUMNS = [
        'fx_accno', 'ccy_code', 'ccy_desc', 'branch_name', 'bank_safecode', 'acctno_status_code', 'acctno_status_desc',
        'acctno_attr_code', 'acctno_attr_desc', 'acctno_limit_cata_code', 'acctno_limit_cata_name', 'acctno_limit'
    ]

    def extract_data(self) -> DataFrame:
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

        tmp = self.spark.sql(f"""
           SELECT acctno as fx_accno, ccy_code, ccy_desc, branch_name, bank_safecode, acctno_status_code, 
                acctno_status_desc,acctno_attr_code, acctno_attr_desc, acctno_limit_cata_code, acctno_limit_cata_name, 
                acctno_limit FROM {table_name1} where dt = '{dt1}'
            union 
                SELECT acctno as fx_accno, ccy_code, ccy_desc, branch_name, bank_safecode,
                acctno_attr_code,acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name2} where dt = '{dt2}'
            union 
                SELECT acctno as fx_accno, ccy_code, branch_name, bank_safecode,
                acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit 
                FROM {table_name3} where dt = '{dt3}'
            union 
                SELECT fx_acctno,'' ccy_code,'' ccy_desc,'' branch_name,'' bank_safecode, 
                '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit
                FROM {table_name4} where dt = '{dt4}'
            union 
                SELECT fx_acctno,'' ccy_code,'' ccy_desc,'' branch_name,'' bank_safecode, 
                '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit
                FROM {table_name5} where dt = '{dt5}'
            union
                SELECT rmb_acctno as fx_accno, ccy_code, branch_name, bank_safecode,
                acctno_attr_code FROM {table_name3} where dt = '{dt3}' and salefx_use_code='022'
            union 
                SELECT oth_acctno as fx_acctno,'' ccy_code,'' ccy_desc,'' branch_name,'' bank_safecode, 
                '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit 
                FROM {table_name4} where dt = '{dt4} and tx_code=929070 and oth_acctno is not null
            union 
                SELECT oth_acctno as fx_acctno,'' ccy_code,'' ccy_desc,'' branch_name,'' bank_safecode, 
                '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name5} where dt = '{dt5} and tx_code=929070 and oth_acctno is not null 
            union
                SELECT acctno as fx_acctno, acct_ccy_code as ccy_code, curr_desc as ccy_desc, branch_name,bank_safecode, 
                '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name6} where dt = '{dt6} 
            union 
                SELECT payer_acctno as fx_acctno, acct_ccy_code as ccy_code, curr_desc as ccy_desc, branch_name,
                bank_safecode2 as bank_safecode, '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name7} where dt = '{dt7} and acct_ccy_code！=“人民币”
            union 
                SELECT rcver_acctno as fx_acctno, '' ccy_code, '' ccy_desc, '' branch_name,
                '' bank_safecode, '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name7} where dt = '{dt7} and acct_ccy_code！=“人民币”
            union 
                SELECT payer_acctno as fx_acctno, acct_ccy_code as ccy_code, curr_desc as ccy_desc, branch_name,
                bank_safecode2 as bank_safecode, '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name7} where dt = '{dt7} and acct_ccy_code=“人民币” and tx_code!=929070
            union 
                SELECT rcver_acctno as fx_acctno, '' ccy_code, '' ccy_desc, '' branch_name,
                '' bank_safecode, '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name7} where dt = '{dt7} and acct_ccy_code=“人民币” and tx_code!=929070
            union 
                SELECT rcver_acctno as fx_acctno, acct_ccy_code as ccy_code, curr_desc as ccy_desc, branch_name,
                bank_safecode2 as bank_safecode, '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name8} where dt = '{dt8} and acct_ccy_code！=“人民币”
            union 
                SELECT payer_acctno as fx_acctno, '' ccy_code, '' ccy_desc, '' branch_name,
                '' bank_safecode, '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name8} where dt = '{dt8} and acct_ccy_code！=“人民币”
            union 
                SELECT rcver_acctno as fx_acctno, acct_ccy_code as ccy_code, curr_desc as ccy_desc, branch_name,
                bank_safecode2 as bank_safecode, '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name8} where dt = '{dt8} and acct_ccy_code=“人民币” and tx_code!=929070
            union 
                SELECT payer_acctno as fx_acctno, '' ccy_code, '' ccy_desc, '' branch_name,
                '' bank_safecode, '' acctno_status_code,'' acctno_status_desc,
                '' acctno_attr_code,'' acctno_attr_desc,'' acctno_limit_cata_code,'' acctno_limit_cata_name,'' acctno_limit  
                FROM {table_name8} where dt = '{dt8} and acct_ccy_code=“人民币” and tx_code!=929070
        """)
        detail_rdd = tmp.rdd.map(lambda x: self.count_nu(x))
        self.spark.createDataFrame(detail_rdd).createOrReplaceTempView('temp_v')
        df = self.spark.sql(f"""
                            select {','.join(self.TABLE_COLUMNS)} 
                            from
                                (select *, 
                                    row_number over(partition by fx_acctno over by nu_count) rn
                                from temp_v)
                            where rn=1
                        """)

        return df

    def count_nu(self, data: Row) -> Row:
        nu_count = 0
        for col in self.TABLE_COLUMNS:
            if not eval('data.{col}'.format(col=col)):
                nu_count += 1

        return Row(fx_acctno=data.fx_acctno,
                   ccy_code=data.ccy_code,
                   ccy_desc=data.ccy_desc,
                   branch_name=data.branch_name,
                   bank_safecode=data.bank_safecode,
                   acctno_status_code=data.acctno_status_code,
                   acctno_status_desc=data.acctno_status_desc,
                   acctno_attr_code=data.acctno_attr_code,
                   acctno_attr_desc=data.acctno_attr_desc,
                   acctno_limit_cata_code=data.acctno_limit_cata_code,
                   acctno_limit_cata_name=data.acctno_limit_cata_name,
                   acctno_limit=data.acctno_limit,
                   nu_count=nu_count)

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_acctno,pboc_corp_buy,pboc_corp_lcy,pboc_corp_pay_jn,pboc_corp_rcv_jn',
            destination_table_name=VertexFxaccount.TABLE_NAME,
            destination_table_columns=VertexFxaccount.TABLE_COLUMNS,
            primary_keys=['fx_accno'],
        )




class VertexRmbaccount(GraphDataExtractTemplate):
    """
    实体表－5:　人民币账户
    """

    TABLE_NAME = 'vertex_rmbaccount'

    TABLE_COLUMNS = ['rmb_accno', 'bank_name']

    def extract_data(self) -> DataFrame:
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

        df = self.spark.sql(f"""
            SELECT rmb_accno, bank_name FROM {table_name1} where dt = '{dt1}'
            union 
            SELECT rmb_accno, bank_name FROM {table_name2} where dt = '{dt2}' and salefx_use_code!='022'
            union 
            SELECT oth_acctno as rmb_accno, bank_name FROM {table_name3} where dt = '{dt3} and tx_code=929070 and oth_acctno is not null'
            union 
            SELECT oth_acctno as rmb_accno, bank_name FROM {table_name4} where dt = '{dt4} and tx_code=929070 and oth_acctno is not null'
            union 
            SELECT rcver_acctno as rmb_accno, bank_name FROM {table_name5} where dt = '{dt5} and is_dom_pay=1 and acct_ccy_code=“人民币” and tx_code=929070
            union 
            SELECT payer_acctno as rmb_accno, bank_name FROM {table_name6} where dt = '{dt6} and is_dom_rcv=1 and acct_ccy_code=“人民币” and tx_code=929070
        """)
        df = df.drop_duplicates(['rmb_accno'])
        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_buy,pboc_corp_lcy,pboc_corp_pay_jn,pboc_corp_rcv_jn',
            destination_table_name=VertexRmbaccount.TABLE_NAME,
            destination_table_columns=VertexRmbaccount.TABLE_COLUMNS,
            primary_keys=['rmb_accno'],
        )



class VertexTransfer(GraphDataExtractTemplate):
    """
    实体表－6:　交易
    """

    TABLE_NAME = 'vertex_transfer'

    TABLE_COLUMNS = ['rptno', 'amt', 'amt_time', 'amt_type', 'amt_comment']

    def extract_data(self) -> DataFrame:
        table_name1 = f'{self.hive_dw_db_name}.pboc_corp_pay_jn'
        dt1 = HiveUtil.newest_partition(self.spark, table_name1)
        table_name2 = f'{self.hive_dw_db_name}.pboc_corp_rcv_jn'
        dt2 = HiveUtil.newest_partition(self.spark, table_name2)
        table_name3 = f'{self.hive_dw_db_name}.pboc_cncc_beps'
        dt3 = HiveUtil.newest_partition(self.spark, table_name3)
        table_name4 = f'{self.hive_dw_db_name}.pboc_cncc_hvps'
        dt4 = HiveUtil.newest_partition(self.spark, table_name4)
        table_name5 = f'{self.hive_dw_db_name}.pboc_cncc_ibps'
        dt5 = HiveUtil.newest_partition(self.spark, table_name5)
        table_name6 = f'{self.hive_dw_db_name}.t_acct_pay_dtl'
        dt6 = HiveUtil.newest_partition(self.spark, table_name6)
        table_name7 = f'{self.hive_dw_db_name}.t_acct_rcv_dtl'
        dt7 = HiveUtil.newest_partition(self.spark, table_name7)
        df = self.spark.sql(f"""
            SELECT 
                rptno, amt,'' amt_time,'' amt_type,'' amt_comment
            FROM 
                (SELECT rptno, tx_amt_usd as amt FROM {table_name1} where dt = '{dt1}' and tx_code=929070 
                union 
                SELECT rptno, '' amt FROM {table_name2} where dt = '{dt2}' and tx_code=929070 
                union 
                SELECT rptno, '' amt FROM {table_name3} where dt = '{dt3}' and is_dom_pay=1 
                union 
                SELECT rptno, '' amt FROM {table_name4} where dt = '{dt4}' and is_dom_pay=1 
                union 
                SELECT rptno, pay_amt_usd as amt FROM {table_name6} where dt = '{dt6}' and is_dom_pay=1 
                union 
                SELECT rptno, rcv_amt_usd as amt FROM {table_name7} where dt = '{dt7}' and is_dom_pay=1 
            ) L
        """)
        df = df.drop_duplicates(['rptno'])

        return df

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='pboc_corp_pay_jn,pboc_corp_rcv_jn,pboc_cncc_beps,pboc_cncc_hvps,pboc_cncc_ibps',
            destination_table_name=VertexTransfer.TABLE_NAME,
            destination_table_columns=VertexTransfer.TABLE_COLUMNS,
            primary_keys=['rptno'],
        )
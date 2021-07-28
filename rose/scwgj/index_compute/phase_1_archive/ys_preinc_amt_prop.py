#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-25 17:44
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="ys_preinc_amt_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v", "pboc_corp_rcv_jn_v"])
def ys_preinc_amt_prop(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")

    # 【对公收入】预收货款交易金额汇总, 交易笔数 【指标计算当天过去一年】
    sql = f'''
        select corp_code, sum(tx_amt_usd) as c13
          from (
                select corp_code, tx_amt_usd, rptno
                  from pboc_corp_rcv_jn_v 
                 where rcvpay_attr_code = 'A'
                 and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 【对公收入】货物贸易收入折美元 【指标计算当天过去一年】
    sql = f'''
        select corp_code, sum(tx_amt_usd) as c1
          from (
                select corp_code, tx_amt_usd, rptno
                  from pboc_corp_rcv_jn_v
                 where tx_code in ('121010', '121020', '121040', '121080', '121090', '121990')
                 and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")
    # 预收货款金额占货物贸易收款金额的比例
    sql = """
        select 
            company.corp_code 
            ,company.company_name 
            ,case when v1.c13 is null and v2.c1 is null then null else coalesce(v1.c13, 1)/coalesce(v2.c1, 1) end ys_preinc_amt_prop
        from target_company_v company
        left join view1 v1 on v1.corp_code = company.corp_code
        left join view2 v2 on v2.corp_code = company.corp_code
        """
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df

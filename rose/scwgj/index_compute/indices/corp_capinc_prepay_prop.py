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


@register(name="corp_capinc_prepay_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_corp_pay_jn_v"])
def corp_capinc_prepay_prop(entry: Entry, param: Parameter, logger):
    """
    二期有更新。
    资本及收入与预付的比例
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")

    # 【对公收入】资本金收入汇总（折美元）【指标计算当天过去一年】
    sql = f"""
            select corp_code, sum(tx_amt_usd) as c23
            from ( 
                select corp_code, tx_amt_usd
                from pboc_corp_rcv_jn_v
                where tx_code in ('622011','622012','622013')
                    and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) 
                                    and date('{pboc_corp_rcv_jn_end_date}')
            )
            group by corp_code
          """
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 【对公付款】预付货款交易金额(汇总)+1 （折美元）【指标计算当天过去一年】
    sql = f"""
        select corp_code, 
            case when c11 is null or c11 = 0 then 1
                 else c11
            end c11
        from (
            select corp_code, sum(tx_amt_usd) as c11 
            from (
                select corp_code, tx_amt_usd
                from pboc_corp_pay_jn_v
                where rcvpay_attr_code = 'A'
                and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) 
                                and date('{pboc_corp_pay_jn_end_date}')
            )
            group by corp_code  
        )
        """
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")

    sql_str = """
                select company.corp_code, 
                    company.company_name, 
                    case when v1.c23 is null and v2.c11 is null then 0
                         else coalesce(v1.c23, 0)/coalesce(v2.c11, 1)
                    end corp_capinc_prepay_prop
                from target_company_v company
                left join view1 v1 on company.corp_code = v1.corp_code
                left join view2 v2 on company.corp_code = v2.corp_code
              """
    logger.info(sql_str)
    df = entry.spark.sql(sql_str)
    return df

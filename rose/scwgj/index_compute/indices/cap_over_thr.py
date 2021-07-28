#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 17:41
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="cap_over_thr",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_lcy_v"])
def cap_over_thr(entry: Entry, param: Parameter, logger):
    """
    指标计算：资本项下备用金结汇超20万。
        1. 【对公付款】当月结汇待支付备用金支付金额（折美元）+【企业结汇】当月备用金结汇（折美元），超过阈值则为1，否则为0；
        2. 【对公付款】结汇待支付：过滤条件为：[交易编码]='929070' and [发票编号] like '%18%'，
           【企业结汇】当月备用金：[结汇用途代码】=‘018’and [账户性质代码] like '2%'；
        3. 指标计算当天过去一年；
        4. 布尔型， 模型输出分布；
        5.  5.1、指标计算值，看分布再定义阈值。
            5.2、指标上线后可通过变更指标脚本修改阈值；
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    pboc_corp_lcy_end_date = t_date_dict.get("pobc_corp_lcy")

    # fixme: 指标最终比较的阈值，由模型计算原始数据分布得出
    the_threshold = 0

    # 【对公付款】当月结汇待支付备用金支付金额（折美元）
    # 结汇待支付：过滤条件为：[交易编码]='929070' and [发票编号] like '%18%'
    temp1_sql = f"""
                    select corp_code, sum(pay_tmat_usd) as pay_sum_amt 
                    from pboc_corp_pay_jn_v
                    where pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) 
                                      and date('{pboc_corp_pay_jn_end_date}')
                        and tx_code = '929070' 
                        and invoice_no like '%18%'
                    group by corp_code
                 """
    logger.info(temp1_sql)
    entry.spark.sql(temp1_sql).createOrReplaceTempView("temp_view_1")

    # 【企业结汇】当月备用金结汇（折美元）
    # 【企业结汇】当月备用金：[结汇用途代码】=‘018’and [账户性质代码] like '2%'
    temp2_sql = f"""
                    select corp_code, sum(salefx_amt_usd) as salefx_sum_amt 
                    from pboc_corp_lcy_v
                    where deal_date between add_months('{pboc_corp_lcy_end_date}', -12) 
                                      and date('{pboc_corp_lcy_end_date}')
                        and salefx_use_code = '018' 
                        and acct_attr_code like '2%' 
                    group by corp_code
                 """
    logger.info(temp2_sql)
    entry.spark.sql(temp2_sql).createOrReplaceTempView("temp_view_2")

    sql_str = f"""
                select corp_code, 
                    company_name, 
                    total_amount as cap_over_thr_value, 
                    case when total_amount > {the_threshold} then 1 
                         else 0 
                    end cap_over_thr
                from (
                    select company.corp_code, 
                        company.company_name, 
                        (coalesce(v1.pay_sum_amt, 0) + coalesce(v2.salefx_sum_amt, 0)) as total_amount
                    from target_company_v company
                    left join temp_view_1 v1 on company.corp_code = v1.corp_code
                    left join temp_view_2 v2 on company.corp_code = v2.corp_code
                )
               """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
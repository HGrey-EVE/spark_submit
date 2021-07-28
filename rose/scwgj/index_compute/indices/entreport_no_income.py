#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 16:14
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="entreport_no_income",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_rcv_jn_v"])
def entreport_no_income(entry: Entry, param: Parameter, logger):
    """
    指标计算：转口贸易只支不收（过去一年）。
        1. 【对公收入】交易金额（折美元）汇总<10000 且 【对公付款】交易金额（折美元）汇总>200000 ；
        2. 统计币种剔除人民币；
        3. 【对公付款】交易编码 in （'122020、121030'）；
        4. 指标计算当天过去一年；
        5. 布尔型，出现过转口贸易只支不收则标记为1，否则为0
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")

    # 【对公收入】交易金额（折美元）汇总<10000
    rcv_sum_trade_sql = f"""
                            select corp_code, sum(new_tx_amt_usd) as sum_rcv_tx_amt_usd
                            from (
                                select corp_code, 
                                    nvl(tx_amt_usd, 0) as new_tx_amt_usd
                                from pboc_corp_rcv_jn_v
                                where rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) 
                                                 and date('{pboc_corp_rcv_jn_end_date}')
                                    and tx_ccy_code != 'CNY' 
                            )   
                            group by corp_code
                         """
    logger.info(rcv_sum_trade_sql)
    entry.spark.sql(rcv_sum_trade_sql).createOrReplaceTempView("rcv_sum_trade_view")

    # 【对公付款】交易金额（折美元）汇总>200000
    pay_sum_trade_sql = f"""
                            select corp_code, sum(new_tx_amt_usd) as sum_pay_tx_amt_usd
                            from (
                                select corp_code, 
                                    nvl(tx_amt_usd, 0) as new_tx_amt_usd
                                from pboc_corp_pay_jn_v
                                where pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) 
                                                 and date('{pboc_corp_pay_jn_end_date}')
                                    and tx_code in ('122020', '121030')
                                    and tx_ccy_code != 'CNY' 
                            )
                            group by corp_code
                         """
    logger.info(pay_sum_trade_sql)
    entry.spark.sql(pay_sum_trade_sql).createOrReplaceTempView("pay_sum_trade_view")

    # 只支不收：pay_sum leftJoin rcv_sum
    pay_without_rcv_sql = """
                            select corp_code,
                                sum_pay_tx_amt_usd,
                                nvl(sum_rcv_tx_amt_usd, 0) as sum_rcv_tx_amt_usd
                            from (
                                select L.corp_code, 
                                    L.sum_pay_tx_amt_usd, 
                                    R.sum_rcv_tx_amt_usd
                                from pay_sum_trade_view L
                                left join rcv_sum_trade_view R
                                on L.corp_code = R.corp_code
                            )
                          """
    logger.info(pay_without_rcv_sql)
    entry.spark.sql(pay_without_rcv_sql).createOrReplaceTempView("pay_without_rcv_view")

    sql_str = """
                select company.corp_code, 
                    company.company_name, 
                    case when pay_rcv.sum_pay_tx_amt_usd > 200000 and pay_rcv.sum_rcv_tx_amt_usd < 10000 then 1
                         else 0
                    end entreport_no_income
                from target_company_v company
                left join pay_without_rcv_view pay_rcv
                on company.corp_code = pay_rcv.corp_code
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df

#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 10:33
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="settle_financing_attr",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_dfxloan_sign_v"])
def settle_financing_attr(entry: Entry, param: Parameter, logger):
    """
    指标计算：结算方式及贸易融资特征。
        1. 【对公收入】(交易性质=“一般贸易收汇" 且 结算方式=“电汇”收款笔数/交易性质=“一般贸易收汇" 的收款笔数>=阈值)
            and
           【国内外汇贷款签约信息】贸易融资笔数>阈值（=0，是否为0先看黑样本）；
        2. 【对公收入】结算方式代码 =‘T’；
        3. 指标计算当天过去一年；
        4. 阈值用黑样本分布计算；
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    pboc_dfxloan_sign_end_date = t_date_dict.get("pboc_dfxloan_sign")

    # todo: 阈值由模型计算得出
    receive_times_ratio_threshold = 0
    trade_financing_threshold = 0

    # 【对公收入】(交易性质=“一般贸易收汇" 且 结算方式=“电汇”收款笔数/交易性质=“一般贸易收汇" 的收款笔数)
    telegraphic_transfer_sql = f"""
                                select corp_code, 
                                    tele_trans_rcv_counts/rcv_counts as tele_trans_ratio
                                from (
                                    select L.corp_code, 
                                        coalesce(L.tele_trans_rcv_counts, 0) as tele_trans_rcv_counts,
                                        case when R.rcv_counts is null or R.rcv_counts = 0 then 1 
                                             else R.rcv_counts
                                        end rcv_counts
                                    from (
                                        select corp_code, count(rcv_tamt) as tele_trans_rcv_counts
                                        from pboc_corp_rcv_jn_v
                                        where tx_code = '121010' 
                                            and settle_method_code = 'T' 
                                            and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) 
                                                            and date('{pboc_corp_rcv_jn_end_date}')
                                        group by corp_code
                                    ) L
                                    join (
                                        select corp_code, count(rcv_tamt) as rcv_counts
                                        from pboc_corp_rcv_jn_v
                                        where tx_code = '121010' 
                                            and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) 
                                                            and date('{pboc_corp_rcv_jn_end_date}')
                                        group by corp_code
                                    ) R
                                    on L.corp_code = R.corp_code
                                )
                                """
    logger.info(telegraphic_transfer_sql)
    entry.spark.sql(telegraphic_transfer_sql).createOrReplaceTempView("telegraphic_transfer_view")

    # 【国内外汇贷款签约信息】贸易融资笔数>阈值（=0，是否为0先看黑样本）；
    # 贸易融资：'国内外汇贷款类型代码' in ('1102')
    trade_financing_sql = f"""
                            select corp_code, count(sign_amt) as trade_financing_times
                            from pboc_dfxloan_sign_v
                            where domfx_loan_type_code = '1102'
                                and interest_start_date between add_months('{pboc_dfxloan_sign_end_date}', -12) 
                                                        and date('{pboc_dfxloan_sign_end_date}')
                            group by corp_code
                           """
    logger.info(trade_financing_sql)
    entry.spark.sql(trade_financing_sql).createOrReplaceTempView("trade_financing_view")

    sql_str = f"""
                select corp_code, 
                    company_name,
                    tele_trans_ratio as settle_financing_attr_value_1, 
                    trade_financing_times as settle_financing_attr_value_2, 
                    case when tele_trans_ratio > {receive_times_ratio_threshold} 
                            and trade_financing_times > {trade_financing_threshold} 
                         then 1 else 0 
                    end settle_financing_attr 
                from (
                    select company.corp_code, 
                        company.company_name, 
                        coalesce(v1.tele_trans_ratio, 0) as tele_trans_ratio,
                        coalesce(v2.trade_financing_times, 0) as trade_financing_times
                    from target_company_v company
                    left join telegraphic_transfer_view v1 on company.corp_code = v1.corp_code
                    left join trade_financing_view v2 on company.corp_code = v2.corp_code
                )  
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
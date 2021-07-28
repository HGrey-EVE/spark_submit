#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 14:30
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="suspect_illegal_rel",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "legal_adjudicative_documents_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_rcv_jn_v"])
def suspect_illegal_rel(entry: Entry, param: Parameter, logger):
    """
    指标计算：疑似非法经营相关联的境内企业。
        1.【裁判文书】筛查案由-含“地下钱庄”的企业名单；
        2.【对公收入】或【对公付款】查询1中企业的境外交易对手；
        3.【对公收入】或【对公付款】判断与上述境外交易对手交易的境内企业是否为该企业，（交易关联）标记境内企业；

        4. 1、2步算存量，第3步指标计算当天过去一年；
        5. 布尔型，有一个关联了则为1，没有关联上的则为0；
        6. 案由为地下钱庄的公司，没有 结果为地下钱庄、也没有正文“地下钱庄”的法人机构，大约50家，已给判决结果内“非法经营”+“外汇”的约75，正在排查正文内“非法经营”+“外汇”正在处理
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")

    # 【裁判文书】筛查案由-含“地下钱庄”的企业名单；
    illegal_company_sql = """
                            select R.corp_code, R.company_name
                            from (
                                select distinct company_name
                                from (
                                    select explode(split(defendant, ';')) as company_name
                                    from legal_adjudicative_documents_v
                                    where action_cause like '%地下钱庄%'
                                )
                            ) L
                            join target_company_v R
                            on L.company_name = R.company_name       
                          """
    logger.info(illegal_company_sql)
    entry.spark.sql(illegal_company_sql).createOrReplaceTempView("illegal_company_view")

    # 【对公收入】或【对公付款】查询1中企业的境外交易对手；
    #  counter_party(交易对手标准名称): 就是境外交易对手 的公司名； 只能通过公司名进行关联
    overseas_counter_party_sql = """
                                    select R.corp_code, L.counter_party
                                    from (
                                        select distinct counter_party
                                        from (
                                            select a.counter_party as counter_party
                                            from pboc_corp_pay_jn_v a
                                            join illegal_company_view b
                                            on a.corp_code = b.corp_code
                                            union all 
                                            select a.counter_party as counter_party
                                            from pboc_corp_rcv_jn_v a
                                            join illegal_company_view b
                                            on a.corp_code = b.corp_code
                                        )
                                    ) L
                                    join target_company_v R
                                    on L.counter_party = R.company_name
                                 """
    logger.info(overseas_counter_party_sql)
    entry.spark.sql(overseas_counter_party_sql).createOrReplaceTempView("overseas_counter_party_view")

    # 【对公收入】或【对公付款】判断与上述境外交易对手交易的境内企业是否为该企业，（交易关联）标记境内企业；
    marked_company_sql = f"""
                            select R.corp_code, L.counter_party
                            from (
                                select distinct counter_party
                                from (
                                    select a.counter_party as counter_party
                                    from (
                                        select corp_code, counter_party
                                        from pboc_corp_pay_jn_v
                                        where pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12)
                                                         and date('{pboc_corp_pay_jn_end_date}')
                                    ) a
                                    join overseas_counter_party_view b
                                    on a.corp_code = b.corp_code
                                    union all 
                                    select a.counter_party as counter_party
                                    from (
                                        select corp_code, counter_party
                                        from pboc_corp_rcv_jn_v
                                        where rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12)
                                                         and date('{pboc_corp_rcv_jn_end_date}')
                                    ) a
                                    join overseas_counter_party_view b
                                    on a.corp_code = b.corp_code
                                )
                            ) L
                            join target_company_v R
                            on L.counter_party = R.company_name                            
                          """
    logger.info(marked_company_sql)
    entry.spark.sql(marked_company_sql).createOrReplaceTempView("marked_company_view")

    sql_str = """
                select L.corp_code, 
                    L.company_name, 
                    case when R.count_number is not null and R.count_number > 0 then 1 
                         else 0
                    end suspect_illegal_rel
                from target_company_v L
                left join (
                    select a.corp_code, count(*) as count_number
                    from marked_company_view a
                    join illegal_company_view b
                    on a.corp_code = b.corp_code
                    group by a.corp_code
                ) R
                on L.corp_code = R.corp_code
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df


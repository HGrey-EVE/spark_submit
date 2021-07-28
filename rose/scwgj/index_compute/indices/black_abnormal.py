#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:40
@Author: zhouchao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="black_abnormal",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["legal_adjudicative_documents_v",
                        "target_company_v",
                        "risk_punishment_v",
                        "legal_persons_subject_to_enforcement_v",
                        "legal_dishonest_persons_subject_to_enforcement_v",
                        "risk_tax_owed_v",
                        "legal_court_notice_v"])
def black_abnormal(entry: Entry, param: Parameter, logger):
    """
    二期更新
    经营是否异常（包含被行政执法部门处罚）。或的关系
        包含行政处罚、被执行人、失信被执行人、法院公告被告、裁判文书被告且败诉、欠税
    """

    # 行政处罚：risk_punishment
    administrative_penalty_sql = """
                                    select distinct company as company_name
                                    from risk_punishment_v
                                 """
    logger.info(administrative_penalty_sql)
    entry.spark.sql(administrative_penalty_sql).createOrReplaceTempView("administrative_penalty_view")

    # 被执行人：legal_persons_subject_to_enforcement;
    # pname_compid =0 表示企业
    person_enforcement_sql = """
                                select distinct pname as company_name
                                from legal_persons_subject_to_enforcement_v 
                                where pname_compid = 0
                             """
    logger.info(person_enforcement_sql)
    entry.spark.sql(person_enforcement_sql).createOrReplaceTempView("person_enforcement_view")

    # 失信被执行人：legal_dishonest_persons_subject_to_enforcement
    dishonest_person_enforcement_sql = """
                                        select distinct pname as company_name
                                        from legal_dishonest_persons_subject_to_enforcement_v  
                                        where pname_compid = 0
                                       """
    logger.info(dishonest_person_enforcement_sql)
    entry.spark.sql(dishonest_person_enforcement_sql).createOrReplaceTempView("dishonest_person_enforcement_view")

    # 人民法院公告：legal_court_notice: 直接取被告信息；
    legal_court_sql = """
                        select distinct company_name 
                        from (
                            select explode(split(defendant, ';')) as company_name
                            from legal_court_notice_v
                        )
                      """
    logger.info(legal_court_sql)
    entry.spark.sql(legal_court_sql).createOrReplaceTempView("legal_court_view")

    # 裁判文书： legal_adjudicative_documents_v
    adjudicative_defendant_failed_sql = """
                                            select distinct company_name 
                                            from (
                                                select explode(split(defendant, ';')) as company_name
                                                from legal_adjudicative_documents_v 
                                                where case_result in ('原告胜诉', '部分胜诉')
                                            )
                                         """
    logger.info(adjudicative_defendant_failed_sql)
    entry.spark.sql(adjudicative_defendant_failed_sql).createOrReplaceTempView("adjudicative_defendant_failed_view")

    # 企业欠税：risk_tax_owed
    tax_owed_sql = """
                    select distinct company_name 
                    from risk_tax_owed_v
                   """
    logger.info(tax_owed_sql)
    entry.spark.sql(tax_owed_sql).createOrReplaceTempView("tax_owed_view")

    sql_str = """
                select company.corp_code, 
                    company.company_name, 
                    case when a.company_name is not null then 1
                         else 0
                    end black_abnormal
                from target_company_v company
                left join (
                    select company_name from administrative_penalty_view
                    union 
                    select company_name from person_enforcement_view
                    union 
                    select company_name from dishonest_person_enforcement_view
                    union 
                    select company_name from legal_court_view
                    union 
                    select company_name from adjudicative_defendant_failed_view
                    union 
                    select company_name from tax_owed_view
                ) a
                on company.company_name = a.company_name
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df

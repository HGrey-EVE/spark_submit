#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/30 : 8:50
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="rel_abnormal_ratio",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "off_line_relations_v",
                        "risk_punishment_v",
                        "legal_persons_subject_to_enforcement_v",
                        "legal_dishonest_persons_subject_to_enforcement_v",
                        "legal_adjudicative_documents_v",
                        "risk_tax_owed_v",
                        "qyxx_jyyc_v"])
def rel_abnormal_ratio(entry: Entry, param: Parameter, logger):
    """
    （失信人、被执行人、工商经营异常、税务违规、裁判文书中的被告、被行政处罚的关联企业数量）/关联企业数量.
    关联方计算方法：
    1. 令source_isperson=0, source_degree=1, 得到list1(company_name, source_name as company_rel_degree_1)
    2. 令destination_isperson=0,destination_degree=1, 得到list2(company_name, destination_name as company_rel_degree_1)
    3. 合并list1和list2，去重，得到list3。company_name为目标企业，company_rel_degree_1为目标企业一度关联方。
    """

    # 一度关联方
    relation_company_sql = """
                            select distinct bbd_qyxx_id, company_name, rel_company_bbd_id, rel_company_name
                            from (
                                select bbd_qyxx_id, 
                                    company_name, 
                                    source_bbd_id as rel_company_bbd_id, 
                                    source_name as rel_company_name
                                from off_line_relations_v
                                where source_isperson = 0 and source_degree = 1
                                union 
                                select bbd_qyxx_id, 
                                    company_name, 
                                    destination_bbd_id as rel_company_bbd_id, 
                                    destination_name as rel_company_name 
                                from off_line_relations_v 
                                where destination_isperson = 0 and destination_degree = 1
                            )
                           """
    logger.info(relation_company_sql)
    entry.spark.sql(relation_company_sql).createOrReplaceTempView("relation_company_view")

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

    # 裁判文书： legal_adjudicative_documents_v
    adjudicative_defendant_failed_sql = """
                                            select distinct company_name 
                                            from (
                                                select explode(split(defendant, ';')) as company_name
                                                from legal_adjudicative_documents_v 
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

    # 企业经营异常
    qyxx_jyyc_sql = """
                        select distinct company_name 
                        from qyxx_jyyc_v 
                    """
    logger.info(qyxx_jyyc_sql)
    entry.spark.sql(qyxx_jyyc_sql).createOrReplaceTempView("qyxx_jyyc_view")

    abnormal_rel_count_sql = """
                                select L.bbd_qyxx_id, count(R.company_name) as abnormal_rel_count
                                from relation_company_view L 
                                join (
                                    select distinct company_name
                                    from (
                                        select company_name from administrative_penalty_view
                                        union 
                                        select company_name from person_enforcement_view
                                        union 
                                        select company_name from dishonest_person_enforcement_view
                                        union 
                                        select company_name from adjudicative_defendant_failed_view
                                        union 
                                        select company_name from tax_owed_view
                                        union 
                                        select company_name from qyxx_jyyc_view
                                    )
                                ) R
                                on L.rel_company_name = R.company_name
                                group by L.bbd_qyxx_id
                             """
    logger.info(abnormal_rel_count_sql)
    entry.spark.sql(abnormal_rel_count_sql).createOrReplaceTempView("abnormal_rel_count_view")

    sql_str = """
                select company.corp_code, 
                    company.company_name, 
                    coalesce (abnormal.abnormal_rel_ratio, 0) as rel_abnormal_ratio
                from target_company_v company
                left join (
                    select bbd_qyxx_id, abnormal_rel_count/rel_count as abnormal_rel_ratio
                    from (
                        select L.bbd_qyxx_id, 
                            coalesce(R.abnormal_rel_count, 0) as abnormal_rel_count, 
                            case when L.rel_count is null or L.rel_count = 0 then 1 
                                 else L.rel_count 
                            end rel_count
                        from (
                            select bbd_qyxx_id, count(rel_company_name) as rel_count
                            from relation_company_view
                            group by bbd_qyxx_id
                        ) L 
                        join abnormal_rel_count_view R on L.bbd_qyxx_id = R.bbd_qyxx_id  
                    )
                ) abnormal
                on company.bbd_qyxx_id = abnormal.bbd_qyxx_id         
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df

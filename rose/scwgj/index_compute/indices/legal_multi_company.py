#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 14:21
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="legal_multi_company",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "qyxx_basic_v",
                        "off_line_relations_v"])
def legal_multi_company(entry: Entry, param: Parameter, logger):
    """
    指标计算：涉及多个公司的法人代表。
        1. BBD数据，标记企业:
            主体信息和关联方信息的数据来源于BBD， 做企业标记；
        2. 2度关联社群内，同法人企业标记;
            二度及二度以内的主体查是否是同一法人，如果是同一法人，则将同法人企业进行标记；
            （注意如果关联方未在监管主体内，无法标记；
             如果关联方本身也是监管主体，则所有同一法人企业都要进行标记）
    """
    """
    todo: 未开始
    假设 A->B->C；
        1. A、B、C 三个公司法人相同，则 A、B、C都是待标记公司；
        2. B、C法人相同，但是A不同。则不做处理。
           因为：首先A法人不同，所以A不标记。然后扫描关联表时，当B作为主体时，能够扫描到 B、C同名。
        3. A、B同名，C不同，则A、B是待标记公司；
        4. A、C同法人，但是B不同。A、C是待标记公司。
    得到待标记公司列表之后，然后查询这些公司是否存在于 qyxx_basic。如果有，才真正标记这个公司，否则不标记。
    """

    """
    二度及二度以内关联方
    select * 
    from off_line_realtions 
    where dt='XXXX' 
        and bbd_qyxx_id='XXXX'
        and (source_degree<=2 or destination_degree<=2);
    """

    # 二度及以内关联的公司（关联方全是公司，没有自然人）
    relation_company_sql = """
                                select bbd_qyxx_id, source_bbd_id as relation_company_bbd_id 
                                from off_line_relations_v 
                                where source_isperson = 0 and source_degree <= 2
                                union 
                                select bbd_qyxx_id, destination_bbd_id as relation_company_bbd_id
                                from off_line_relations_v 
                                where destination_isperson = 0 and destination_degree <= 2
                           """
    logger.info(relation_company_sql)
    entry.spark.sql(relation_company_sql).createOrReplaceTempView("relation_company_view")

    # source_isperson=1 and relation_type = 'LEGAL' and position='法人'
    company_frname_sql = """
                    select bbd_qyxx_id, source_name as frname
                    from off_line_relations_v 
                    where source_isperson = 1 and (relation_type = 'LEGAL' or position = '法人')
                 """
    logger.info(company_frname_sql)
    entry.spark.sql(company_frname_sql).createOrReplaceTempView("company_frname_view")

    # 主体和关联方同法人的bbd_qyxx_id
    same_frname_sql = """
                        select main_company.bbd_qyxx_id, relation_company.bbd_qyxx_id as relation_company_bbd_id
                        from (
                            select R.bbd_qyxx_id, frname, L.relation_company_bbd_id 
                            from relation_company_view L 
                            join company_frname_view R on L.bbd_qyxx_id = R.bbd_qyxx_id
                        ) main_company
                        join (
                            select R.bbd_qyxx_id, frname, L.relation_company_bbd_id 
                            from relation_company_view L 
                            join company_frname_view R on L.relation_company_bbd_id = R.bbd_qyxx_id
                        ) relation_company
                        on main_company.relation_company_bbd_id = relation_company.bbd_qyxx_id 
                            and main_company.frname = relation_company.frname
                      """
    logger.info(same_frname_sql)
    entry.spark.sql(same_frname_sql).createOrReplaceTempView("same_frname_view")

    # 标记target_company_v 中的企业
    sql_str = """
                select company.corp_code, 
                    company.comany_name, 
                    case when marked_bbd_company.bbd_qyxx_id is not null then 1 else 0 
                    end legal_multi_company
                from target_company_v company
                left join (
                    select distinct L.bbd_qyxx_id
                    from (
                        select distinct bbd_qyxx_id from same_frname_view 
                        union
                        select distinct relation_company_bbd_id as bbd_qyxx_id from same_frname_view
                    ) L 
                    join qyxx_basic_v R 
                    on L.bbd_qyxx_id = R.bbd_qyxx_id 
                ) marked_bbd_company 
                on company.bbd_qyxx_id = marked_bbd_company.bbd_qyxx_id 
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)

    return result_df


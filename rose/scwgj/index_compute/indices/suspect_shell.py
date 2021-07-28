#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 15:40
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="suspect_shell",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "qyxx_basic_v",
                        "pboc_corp_v",
                        "manage_recruit_v",
                        "manage_tender_v",
                        "manage_bidwinning_v",
                        "qyxx_bgxx_merge_v"])
def suspect_shell(entry: Entry, param: Parameter, logger):
    """
    指标计算：疑似空壳公司。
    查有效境内非金融机构信息，如果非金融机构表的法人字段为空，才关联查询qyxx_basic
        1.（
            【有效境内非金融机构表】法定代表人名称=联系人名称： 关联字段-通过公司id（问黄俞均），而不是公司名。
            或
            【bbd_qyxx_basic】同法人、同联系人
          ）
          且
        （【bbd_qyxx_basic】历史无招聘、无招投标（投标直接计算为中标）、无企业变更。 这个条件是且）;
    """

    # 1. 同法人联系人
    same_contact_frname_sql = """
                                select distinct corp_code 
                                from pboc_corp_v 
                                where frname is not null 
                                    and frname = contact_name
                                union 
                                select distinct L.corp_code as corp_code
                                from (
                                    select corp_code, bbd_qyxx_id 
                                    from pboc_corp_v 
                                    where frname is null or frname = ''
                                ) L 
                                join qyxx_basic_v R 
                                on L.bbd_qyxx_id = R.bbd_qyxx_id
                              """
    logger.info(same_contact_frname_sql)
    entry.spark.sql(same_contact_frname_sql).createOrReplaceTempView("same_contact_frname_view")

    # 2. 无招聘。manage_recruit中无数据的bbd_qyxx_id
    # todo: sql not in 待优化
    non_recruit_sql = """
                        select distinct L.corp_code
                        from pboc_corp_v L
                        join (
                            select distinct bbd_qyxx_id
                            from qyxx_basic_v 
                            where bbd_qyxx_id not in (
                                select distinct bbd_qyxx_id from manage_recruit_v
                            )
                        ) R
                        on L.bbd_qyxx_id = R.bbd_qyxx_id
                      """
    logger.info(non_recruit_sql)
    entry.spark.sql(non_recruit_sql).createOrReplaceTempView("non_recruit_view")

    # 3. 无招标且无中标
    # todo: sql not in 待优化.
    non_invite_and_bid_sql = """
                                select distinct L.corp_code
                                from pboc_corp_v L 
                                join (
                                    select bbd_qyxx_id
                                    from qyxx_basic_v 
                                    where company_name not in (
                                        select distinct company_name
                                        from (
                                            select company_name_win as company_name from manager_tender
                                            union 
                                            select company_name_invite as company_name from manager_tender
                                            union 
                                            select company_name_win as company_name from manage_bidwinning
                                            union 
                                            select company_name_invite as company_name from manage_bidwinning
                                        )
                                    )
                                ) R 
                                on L.bbd_qyxx_id = R.bbd_qyxx_id
                             """
    logger.info(non_invite_and_bid_sql)
    entry.spark.sql(non_invite_and_bid_sql).createOrReplaceTempView("non_invite_bid_view")

    # 4. 企业变更信息表中无数据的bbd_qyxx_id. qyxx_bgxx_merge
    non_bgxx_sql = """
                    select distinct L.corp_code 
                    from pboc_corp_v L 
                    join (
                        select distinct bbd_qyxx_id 
                        from qyxx_basic_v 
                        where bbd_qyxx_id not in (
                            select distinct bbd_qyxx_id from qyxx_bgxx_merge_v
                        )
                    ) R 
                    on L.bbd_qyxx_id = R.bbd_qyxx_id
                   """
    logger.info(non_bgxx_sql)
    entry.spark.sql(non_bgxx_sql).createOrReplaceTempView("non_bgxx_view")

    # 疑似空壳公司
    sql_str = """
                select company.corp_code, 
                    company.company_name,
                    case when v1.corp_code is not null 
                            and v2.corp_code is not null 
                            and v3.corp_code is not null 
                            and v4.corp_code is not null 
                         then 1 else 0
                    end suspect_shell
                from target_company_v company
                left join same_contact_frname_view v1 on company.corp_code = v1.corp_code
                left join non_recruit_view v2 on company.corp_code = v2.corp_code
                left join non_invite_bid_view v3 on company.corp_code = v3.corp_code
                left join non_bgxx_view v4 on company.corp_code = v3.corp_code
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df




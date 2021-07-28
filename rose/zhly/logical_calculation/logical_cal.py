#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 智慧楼宇企业名录
:Author: zhouchao@bbdservice.com
:Date: 2021-05-06 16:08
"""
import os

from whetstone.core.entry import Entry
from zhly.proj_common.hive_util import HiveUtil
from pyspark.sql import SparkSession
from zhly.proj_common.common_util import save_text_to_hdfs
from zhly.proj_common.date_util import DateUtils
import pyspark.sql.functions as F

def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    spark: SparkSession = entry.spark

    # 项目共有配置
    hive_table = entry.cfg_mgr.get("hive", "database")
    entry.logger.info(f"配置信息hive_db:{hive_table}")

    now_str = DateUtils.now2str(fmt='%Y-%m-%d')
    # 时间维度
    premon = DateUtils.add_date_2str_4str(now_str, months=-1)[:-3]
    lastmon = DateUtils.add_date_2str_4str(now_str, months=-2)[:-3]
    # 获取数仓表数据
    # qyxx_basic
    spark.sql(f"""
            select * from 
                {hive_table}.qyxx_basic 
              where dt={HiveUtil.newest_partition(spark, f"{hive_table}.qyxx_basic")}
                and address not rlike '天府新区'
        """).createOrReplaceTempView('qyxx_basic')
    # qyxx_fzjg_merge
    spark.sql(f"""
            select * from {hive_table}.qyxx_fzjg_merge 
                where dt={HiveUtil.newest_partition(spark, f"{hive_table}.qyxx_fzjg_merge")}
        """).createOrReplaceTempView('qyxx_fzjg_merge')

    # manage_recruit
    spark.sql(f"""
            select *
                  from {hive_table}.manage_recruit
                where dt = {HiveUtil.newest_partition(spark, f"{hive_table}.manage_recruit")}
        """).createOrReplaceTempView('manage_recruit')
    # ktgg
    spark.sql(f"""
            select * 
              from 
                (select *, 
                    row_number() over(partition by case_code,bbd_qyxx_id order by trial_date desc) as rn
                from {hive_table}.ktgg 
                where dt={HiveUtil.newest_partition(spark, f"{hive_table}.ktgg")}
                      and case_code is not null 
                      and bbd_xgxx_id is not null
                    )
              where rn=1
        """).drop('rn').createOrReplaceTempView('ktgg_v')
    # sxbzx
    spark.sql(f"""
           select * from
                (select *, row_number() over(partition by bbd_qyxx_id,case_code,pname order by register_date desc) as rn
                   from {hive_table}.legal_persons_subject_to_enforcement
               where dt={HiveUtil.newest_partition(spark, f"{hive_table}.legal_persons_subject_to_enforcement")} 
                    and source_key='pname_origin'
                    and case_code is not null 
                    and bbd_qyxx_id is not null
                    )
           where rn=1
           """
    ).drop('rn').createOrReplaceTempView('sxbzx_v')
    # qyxx_bgxx_merge_clean
    spark.sql(f"""
            select * 
            from 
                {hive_table}.qyxx_bgxx_merge_clean
            where change_key == 'regcap'""").createOrReplaceTempView('qyxx_bgxx_merge_clean')

    # off_line_relations

    in_degree_2_in = spark.sql(
        f"""
            select distinct bbd_qyxx_id, source_bbd_id as relation_id,source_isperson as isperson ,
                    source_name as relation_name
                from {hive_table}.off_line_relations 
            where dt={HiveUtil.newest_partition(spark, f"{hive_table}.off_line_relations", in_month_str=lastmon)} and source_degree<=2
            """
    )
    in_degree_2_out = spark.sql(
        f"""
            select bbd_qyxx_id,destination_bbd_id as relation_id,destination_isperson as isperson,
            destination_name as relation_name
              from {hive_table}.off_line_relations 
            where dt={HiveUtil.newest_partition(spark, f"{hive_table}.off_line_relations", in_month_str=lastmon)} and destination_degree<=2
            """
    )
    in_degree_2_in.union(in_degree_2_out).dropDuplicates().createOrReplaceTempView('glf_last')

    in_degree_2_in_last = spark.sql(
        f"""
                select distinct bbd_qyxx_id, source_bbd_id as relation_id,source_isperson as isperson ,
                        source_name as relation_name
                    from {hive_table}.off_line_relations 
                where dt={HiveUtil.newest_partition(spark, f"{hive_table}.off_line_relations", in_month_str=premon)} and source_degree<=2
                """
    )
    in_degree_2_out_last = spark.sql(
        f"""
            select bbd_qyxx_id,destination_bbd_id as relation_id,destination_isperson as isperson,
            destination_name as relation_name
              from {hive_table}.off_line_relations 
            where dt={HiveUtil.newest_partition(spark, f"{hive_table}.off_line_relations", in_month_str=premon)} and destination_degree<=2
            """
    )
    in_degree_2_in_last.union(in_degree_2_out_last).dropDuplicates().createOrReplaceTempView('glf_now')

    # zpzzq
    spark.sql(f"""
        select *
            from {hive_table}.zpzzq
        where
            dt = {HiveUtil.newest_partition(spark, f"{hive_table}.zpzzq")}
        """).createOrReplaceTempView('zpzzq')
    # xgxx_shangbiao
    spark.sql(f"""
            select * 
                from {hive_table}.xgxx_shangbiao
            where dt={HiveUtil.newest_partition(spark, f"{hive_table}.xgxx_shangbiao")}
            """).createOrReplaceTempView('xgxx_shangbiao')

    # 获取指标数据
    entry.logger.info("开始计算指标数据")
    cal_index_data(entry, spark)
    entry.logger.info("计算指标数据完成")

    # 获取潜在招商企业表
    entry.logger.info("开始计算潜在招商企业数据")
    zsjz_qy_data(entry, spark)
    entry.logger.info("潜在招商企业数据数据完成")

def cal_index_data(entry, spark):
    # 获取配置信息
    index_path = entry.cfg_mgr.hdfs.get_result_path("zhly", "index_hdfs_path")
    company_path = entry.cfg_mgr.hdfs.get_input_path("zhly", "ly_company_path")
    now_str = DateUtils.now2str(fmt='%Y-%m-%d')
    # 时间维度
    one_month_ago = DateUtils.add_date_2str_4str(now_str, months=-1)
    premon = one_month_ago[:-3]
    three_month_ago = DateUtils.add_date_2str_4str(now_str, months=-3)
    six_month_ago = DateUtils.add_date_2str_4str(now_str, months=-6)
    one_year_ago = DateUtils.add_date_2str_4str(now_str, years=-1)
    # 获取楼宇企业名录
    ly_company = spark.read.parquet(company_path)
    ly_company.cache().createOrReplaceTempView('basic_tmp')
    # 招聘人数异动
    entry.logger.info('开始计算招聘人数异动指标')
    spark.sql("""
            select b.* from
              basic_tmp a
            join
              qyxx_basic b
            on a.bbd_qyxx_id=b.bbd_qyxx_id
        """).createOrReplaceTempView('qyw_company')
    spark.sql("""
            select L.bbd_qyxx_id, R.bbd_branch_id from 
              qyw_company L 
            join 
              qyxx_fzjg_merge R 
            on L.bbd_qyxx_id = R.bbd_qyxx_id
        """).createOrReplaceTempView('fzjg_company')

    spark.sql(f"""
            select bbd_qyxx_id, sum(bbd_recruit_num) as recruit_num 
              from
                (select bbd_qyxx_id, nvl(bbd_recruit_num, 0) as bbd_recruit_num
                  from
                  manage_recruit
                  where substring(pubdate, 1, 7)="{premon}")
              group by bbd_qyxx_id
        """).createOrReplaceTempView('recruit_tmp')

    zprsyd = spark.sql("""
            select b.bbd_qyxx_id, nvl(b.recruit_num, 0) - nvl(a.recruit_num, 0) as zprsyd
              from (
                select qc.bbd_qyxx_id, re.recruit_num
                    from
                        qyw_company qc
                    left join recruit_tmp re
                    on qc.bbd_qyxx_id=re.bbd_qyxx_id         
                )a
              right join (
                select fz.bbd_qyxx_id, sum(recruit_num) as recruit_num
                    from
                        fzjg_company fz
                    left join recruit_tmp re1
                    on fz.bbd_branch_id=re1.bbd_qyxx_id  
                    group by fz.bbd_qyxx_id       
                )b
            on a.bbd_qyxx_id=b.bbd_qyxx_id
        """)
    entry.logger.info('招聘人数异动指标计算完成')
    zprsyd = zprsyd.join(ly_company.drop('company_name'), 'bbd_qyxx_id', 'right').fillna(0)
    save_text_to_hdfs(df=zprsyd, path=os.path.join(index_path,'zprsyd'), repartition_number=10, force_repartition=True)

    # 企业开庭公告数量近三个月环比增长率
    entry.logger.info("开始计算企业开庭公告数量近三个月环比增长率指标数据")
    spark.sql("""
        select b.bbd_qyxx_id, b.company_name, a.bbd_xgxx_id, a.case_code, a.pubdate 
            from ktgg_v a
            right join basic_tmp b
            on a.bbd_qyxx_id=b.bbd_qyxx_id
        """
    ).createOrReplaceTempView('ktgg')
    qy_ktgg_rate_3monthhbzzl = spark.sql(f"""
        select b.bbd_qyxx_id, (b.ktgg_num -nvl(a.ktgg_num, 0))/nvl(a.ktgg_num, 1) as qy_ktgg_rate_3monthhbzzl
            from
            (select bbd_qyxx_id, count(distinct bbd_xgxx_id, case_code) as ktgg_num
                from ktgg
            where pubdate between date("{six_month_ago}") and date("{three_month_ago}")
                group by bbd_qyxx_id)  a
            right join 
            (select bbd_qyxx_id, count(distinct bbd_xgxx_id, case_code) as ktgg_num
                from ktgg
            where pubdate between date("{three_month_ago}") and date("{now_str}")
                group by bbd_qyxx_id)  b
            on a.bbd_qyxx_id=b.bbd_qyxx_id
        """)
    entry.logger.info("计算企业开庭公告数量近三个月环比增长率指标数据完成")
    qy_ktgg_rate_3monthhbzzl = qy_ktgg_rate_3monthhbzzl.join(ly_company.drop('company_name'), 'bbd_qyxx_id', 'right').fillna(0)
    save_text_to_hdfs(df=qy_ktgg_rate_3monthhbzzl, path=os.path.join(index_path, 'qy_ktgg_rate_3monthhbzzl'), repartition_number=10, force_repartition=True)
    # 企业失信被执行人数量近三个月环比增长率
    entry.logger.info("开始计算企业失信被执行人数量近三个月环比增长率指标数据")
    spark.sql(
        """
        select b.bbd_qyxx_id, b.company_name, a.bbd_xgxx_id, a.case_code, a.register_date 
            from 
                sxbzx_v a
            right join basic_tmp b
            on a.pname rlike b.company_name
        """
    ).createOrReplaceTempView('sxbzx')
    qy_sxbzxr_rate_3monthhbzzl = spark.sql(f"""
            select b.bbd_qyxx_id, (b.bzx_num -nvl(a.bzx_num, 0))/nvl(a.bzx_num, 1) as qy_sxbzxr_rate_3monthhbzzl
                from
                (select bbd_qyxx_id, count(distinct bbd_xgxx_id, case_code) as bzx_num
                    from sxbzx
                where register_date between date("{six_month_ago}") and date("{three_month_ago}")
                    group by bbd_qyxx_id)  a
                right join 
                (select bbd_qyxx_id, count(distinct bbd_xgxx_id, case_code) as bzx_num
                    from sxbzx
                where register_date between date("{three_month_ago}") and date("{now_str}")
                    group by bbd_qyxx_id)  b
                on a.bbd_qyxx_id=b.bbd_qyxx_id
            """)
    entry.logger.info("计算企业失信被执行人数量近三个月环比增长率数据完成")
    qy_sxbzxr_rate_3monthhbzzl = qy_sxbzxr_rate_3monthhbzzl.join(ly_company.drop('company_name'), 'bbd_qyxx_id', 'right').fillna(0)
    save_text_to_hdfs(df=qy_sxbzxr_rate_3monthhbzzl, path=os.path.join(index_path, 'qy_sxbzxr_rate_3monthhbzzl'), repartition_number=10,
                      force_repartition=True)

    # 注册资本变更总额_当月
    entry.logger.info("开始计算注册资本变更总额_当月数据")
    zczbbg_amount_dy = spark.sql(f"""
            select a.bbd_qyxx_id, nvl((content_after_change - content_before_change), 0) as zczbbg_amount_dy
              from basic_tmp a left join
                (select * from  qyxx_bgxx_merge_clean 
                    where substring(change_date, 1, 7) = "{premon}") b
              on a.bbd_qyxx_id=b.bbd_qyxx_id
        """)
    zczbbg_amount_dy = zczbbg_amount_dy.join(ly_company.drop('company_name'), 'bbd_qyxx_id', 'right').fillna(0)
    save_text_to_hdfs(df=zczbbg_amount_dy, path=os.path.join(index_path ,'zczbbg_amount_dy'), repartition_number=10,
                      force_repartition=True)
    # 二度以内关联方数量变化_当月
    entry.logger.info("开始计算二度以内关联方数量变化_当月数据")

    degree_num_change = spark.sql("""
            select now_glf.bbd_qyxx_id, nvl(now_glf.glf_num, 0) - nvl(last_glf.glf_num, 0) as 2degree_num_change 
              from
                (select a.bbd_qyxx_id, count(distinct relation_id) as glf_num from 
                    basic_tmp a left join glf_now b on a.bbd_qyxx_id=b.bbd_qyxx_id
                    group by a.bbd_qyxx_id
                    ) now_glf left join
                (select c.bbd_qyxx_id, count(distinct relation_id) as glf_num from 
                    basic_tmp c left join glf_last d on c.bbd_qyxx_id=d.bbd_qyxx_id
                    group by c.bbd_qyxx_id
                    ) last_glf
              on now_glf.bbd_qyxx_id=last_glf.bbd_qyxx_id
        """)
    degree_num_change = degree_num_change.join(ly_company.drop('company_name'), 'bbd_qyxx_id', 'right').fillna(0)
    save_text_to_hdfs(df=degree_num_change, path=os.path.join(index_path, '2degree_num_change'), repartition_number=10,
                      force_repartition=True)

    # 企业作品著作数量近三个月环比增长率
    entry.logger.info("开始计算企业作品著作数量近三个月环比增长率数据")
    spark.sql(f"""
                select a.bbd_qyxx_id, b.bbd_xgxx_id, creation_completion_date
                  from basic_tmp a left join
                    zpzzq b
                  on b.copyright_owner rlike a.company_name
            """).createOrReplaceTempView('qy_zpzz')

    qy_zpzz_rate_3monthhbzzl = spark.sql(f"""
            select b.bbd_qyxx_id, (b.bzx_num -nvl(a.bzx_num, 0))/nvl(a.bzx_num, 1) as qy_zpzz_rate_3monthhbzzl
                from
                (select bbd_qyxx_id, count(distinct bbd_xgxx_id) as bzx_num
                    from qy_zpzz
                where creation_completion_date between date("{six_month_ago}") and date("{three_month_ago}")
                    group by bbd_qyxx_id)  a
                right join 
                (select bbd_qyxx_id, count(distinct bbd_xgxx_id) as bzx_num
                    from qy_zpzz
                where creation_completion_date between date("{three_month_ago}") and date("{now_str}")
                    group by bbd_qyxx_id)  b
                on a.bbd_qyxx_id=b.bbd_qyxx_id
            """)
    qy_zpzz_rate_3monthhbzzl = qy_zpzz_rate_3monthhbzzl.join(ly_company.drop('company_name'), 'bbd_qyxx_id', 'right').fillna(0)
    save_text_to_hdfs(df=qy_zpzz_rate_3monthhbzzl, path=os.path.join(index_path,'qy_zpzz_rate_3monthhbzzl'), repartition_number=10,
                      force_repartition=True)
    # 企业作品著作数量近一年
    entry.logger.info("开始企业作品著作数量近一年数据")
    qy_zpzz_num_1year = spark.sql(f"""
                select a.bbd_qyxx_id, count(distinct b.bbd_xgxx_id) as qy_zpzz_num_1year
                  from basic_tmp a left join
                    (select * 
                        from qy_zpzz
                    where creation_completion_date > date("{one_year_ago}")
                    ) b
                  on a.bbd_qyxx_id=b.bbd_qyxx_id
                group by a.bbd_qyxx_id
            """)

    save_text_to_hdfs(df=qy_zpzz_num_1year, path=os.path.join(index_path, 'qy_zpzz_num_1year'), repartition_number=10,
                      force_repartition=True)

    # 企业商标数量近三个月环比增长率
    entry.logger.info("开始企业商标数量近三个月环比增长率数据")
    spark.sql(f"""
                    select a.bbd_qyxx_id, b.bbd_xgxx_id, reg_notice_date
                      from basic_tmp a left join
                        xgxx_shangbiao b
                      on a.bbd_qyxx_id=b.bbd_qyxx_id
                """).createOrReplaceTempView('qy_shangbiao')

    qy_sb_num_dy = spark.sql(f"""
                select b.bbd_qyxx_id, (b.bzx_num -nvl(a.bzx_num, 0))/nvl(a.bzx_num, 1) as qy_sb_num_dy
                    from
                    (select bbd_qyxx_id, count(distinct bbd_xgxx_id) as bzx_num
                        from qy_shangbiao
                    where reg_notice_date between date("{six_month_ago}") and date("{three_month_ago}")
                        group by bbd_qyxx_id)  a
                    right join 
                    (select bbd_qyxx_id, count(distinct bbd_xgxx_id) as bzx_num
                        from qy_shangbiao
                    where reg_notice_date between date("{three_month_ago}") and date("{now_str}")
                        group by bbd_qyxx_id)  b
                    on a.bbd_qyxx_id=b.bbd_qyxx_id
                """)
    qy_sb_num_dy = qy_sb_num_dy.join(ly_company.drop('company_name'), 'bbd_qyxx_id', 'right').fillna(0)
    save_text_to_hdfs(df=qy_sb_num_dy, path=os.path.join(index_path, 'qy_sb_num_dy'), repartition_number=10,
                      force_repartition=True)

def zsjz_qy_data(entry, spark):
    # 潜在招商企业表
    out_path = entry.cfg_mgr.hdfs.get_result_path("zhly", "zsjz_index")
    no_fzjg_qy = spark.sql("""
           select a.bbd_qyxx_id, b.bbd_branch_id,  'qzqy_data' as bbd_table, 'qzqy_data' as bbd_type,
                    a.bbd_uptime, a.bbd_dotime from basic_tmp a left join qyxx_fzjg_merge b
           on a.bbd_qyxx_id=b.bbd_qyxx_id
        """).where('bbd_branch_id is null')
    spark.sql(f"""
                select bbd_recruit_num, bbd_qyxx_id, bbd_salary,
                      case when education_required rlike '高中|中专|初中' then "high_school"
                           when education_required rlike '本科|大学|大专|全日制' then "university"
                           when education_required rlike '研究生|硕士|博士|博士后|海归' then "graduate"
                      end  edu
                    from manage_recruit
                    where job_address rlike '天府新区'
                    and bbd_qyxx_id is not null
                    and bbd_salary is not null
                    and bbd_recruit_num is not null
            """).where('edu is not null').createOrReplaceTempView('recruit_tmp')
    if no_fzjg_qy:
        no_fzjg_qy.createOrReplaceTempView('no_fzjg_qy')
        salary = spark.sql("""
                select a.bbd_qyxx_id, max(b.bbd_salary) as max_salary, min(b.bbd_salary) as min_salary
                  from no_fzjg_qy a
                join
                  recruit_tmp b
                on a.bbd_qyxx_id=b.bbd_qyxx_id
                group by a.bbd_qyxx_id
                    """)
        recruit_count = spark.sql("""
               select sum(bbd_recruit_num) as recruit_num, bbd_qyxx_id, edu
                      from recruit_tmp
                    group by bbd_qyxx_id, edu
            """)

        recruit = recruit_count.groupby('bbd_qyxx_id')\
            .pivot("edu", ['high_school', 'university', 'graduate'])\
            .agg(F.sum('recruit_num')) \
            .fillna(0)

        qz_zsjzz = no_fzjg_qy.drop('bbd_branch_id').join(salary, 'bbd_qyxx_id', 'left')\
            .fillna(0).join(recruit, 'bbd_qyxx_id', 'left').fillna(0)
        qz_zsjzz.repartition(10).write.json(out_path, mode='overwrite')


def post_check(entry: Entry):
    return True

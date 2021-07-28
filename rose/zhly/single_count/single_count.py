#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 智慧楼宇单独指标计算
"""
import traceback

from pyspark.sql import SparkSession, DataFrame
import datetime
import os

from whetstone.core.entry import Entry
from zhly.proj_common.hive_util import HiveUtil
from zhly.proj_common.common_util import save_text_to_hdfs
from zhly.proj_common.date_util import DateUtils


def post_check(entry: Entry):
    return True


def pre_check(entry: Entry):
    return True


# #测试时单个方法失败后继续执行
# def print_exception_handler(f):
#
#     def left(*args, **kwargs):
#         method_name = f.__name__
#         try:
#             print(f'begin exec method: {method_name}')
#             ret = f(*args, **kwargs)
#             print(f'exec method:{method_name} success')
#         except Exception:
#             print(f'exec method {method_name} failed:{traceback.format_exc()}')
#
#     return left



def hive_table_init(
        spark,
        tablename: str,
        new_table=None,
        database='dw',
        dateNo=None
):
    """
    初始化hive表
    :param spark:
    :param tablename:需要初始化的hive表
    :param new_table:新建的临时表名(默认和原hive表同名)
    :param database:
    :param dateNo: 在某个月的最新分区,注意日期格式要和分区格式一致，eg:201901,2019-01
    """
    if new_table is None:
        new_table = tablename

    if dateNo is None:
        spark.sql('''
            select a.* from {table} a  where dt = {new_par}
        '''.format(new_par=HiveUtil.newest_partition(spark, f'{database}.{tablename}'),
                   table=f'{database}.{tablename}')) \
            .createOrReplaceTempView(f'{new_table}')
    else:
        spark.sql('''
             select a.* from {table} a  where dt = {new_par}
        '''.format(
            new_par=HiveUtil.newest_partition_by_month(spark, table=f'{database}.{tablename}',
                                                       in_month_str=str(dateNo)),
            table=f'{database}.{tablename}')) \
            .createOrReplaceTempView(f'{new_table}')


class SingerCount:

    ##初始化
    def __init__(self, entry: Entry):
        # 由于每月初执行。所以当月取上月，以此类推
        self.now_mon = DateUtils.now2str(fmt="%Y-%m")
        self.mon = DateUtils.add_date_4str(self.now_mon, in_fmt='%Y-%m', months=-1)
        self.premon = DateUtils.add_date_4str(self.now_mon, in_fmt='%Y-%m', months=-2)
        self.savepath = entry.cfg_mgr.hdfs.get_result_path("zhly", "index_hdfs_path")

    # #变动前区域 输出qyxx_baisc中企业上月的注册地址address
    def get_bdq_qy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            b.address as bdq_qy
            from all_company a 
            left join  
            qyxx_basic_pre b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
        ''')

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'bdq_qy'))

        entry.logger.info("第一部分：变动前区域执行完成")

    # #变动后区域 输出qyxx_baisc中企业当月的注册地址address
    def get_bdh_qy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            b.address as bdh_qy
            from all_company a 
            left join  
            qyxx_basic b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by a.bbd_qyxx_id, b.address
        ''')
        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'bdh_qy'))
        entry.logger.info("第二部分：变动后区域执行完成")

    # #负面舆情异动
    def get_fmyqyd(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select a.bbd_qyxx_id,
            b.gen_senti as fmyqyd
            from all_company a 
            left join 
            (select * from
                (select *,
                row_number() over(partition by bbd_qyxx_id order by gen_senti desc) as rn
                from
                news_public_sentiment)
            where rn=1) b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by a.bbd_qyxx_id,b.gen_senti
        ''')

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'fmyqyd'))

        entry.logger.info("第三部分：负面舆情异动执行完成")

    # #裁判文书数量——当月
    # 匹配目标公司名称在表dw.legal_adjudicative_documents的数据，sentence_date为当月
    # 然后按照case_code_unique去重计数
    def get_cpws_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.bbd_qyxx_id is not null then b.case_code_unique end) as qy_cpws_dy
            from all_company a 
            left join
            legal_adjudicative_documents b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id 
            and substring(b.sentence_date,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))
        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_cpws_dy'))
        entry.logger.info("第四部分：裁判文书数量-当月执行完成")

    # #开庭公告数量-当月
    def get_qy_ktgg_dy(self, entry: Entry):
        str1 = '''
        select 
        a.bbd_qyxx_id,
        count(distinct case when b.litigant is not null then b.case_code end) as qy_ktgg_dy
        from all_company a 
        left join 
        ktgg b 
        on a.company_name like concat('%%',b.litigant,'%%')
        and substring(b.pubdate,1,7) = '{mon}'
        group by a.bbd_qyxx_id
    '''.format(mon=self.mon)
        entry.logger.info(f'当前sql为:{str1}'.format(str1=str1))
        rdd1 = entry.spark.sql(str1)
        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_ktgg_dy'))

        entry.logger.info("第五部分：开庭公告数量-当月执行完成")

    # #企业失信被执行人数量-当月
    # 筛选出目标公司名称在表dw.legal_persons_subject_to_enforcement的字段pname，按照rigister_date统计当月的数据条数
    def get_qy_sxbzxr_rate_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select
            a.bbd_qyxx_id,
            sum(case when b.pname is not null then 1 else 0 end) as qy_sxbzxr_rate_dy
            from
            all_company a 
            left join
            legal_persons_subject_to_enforcement b 
            on a.company_name like concat('%%',b.pname,'%%') 
            and substring (b.register_date,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_sxbzxr_rate_dy'))

        entry.logger.info("第六部分：企业失信被执行人数量-当月执行完成")

    # #企业严重违法失信数量
    def get_qy_yzwfsx_num(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.bbd_qyxx_id is not null then b.bbd_unique_id end) as qy_yzwfsx_num
            from all_company a 
            left join  
            qyxx_yzwf b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by a.bbd_qyxx_id
        ''')

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_yzwfsx_num'))

        entry.logger.info("第7部分：企业严重违法失信数量执行完成")

    # #企业严重违法失信数量——当月
    def get_qy_yzwfsx_num_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
               select 
               a.bbd_qyxx_id,
               count(distinct case when b.bbd_qyxx_id is not null then b.bbd_unique_id end) as qy_yzwfsx_num_dy
               from all_company a 
               left join  
               qyxx_yzwf b
               on a.bbd_qyxx_id = b.bbd_qyxx_id
               and substring (b.rank_date,1,7) = '{mon}'
               group by a.bbd_qyxx_id
           '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_yzwfsx_num_dy'))

        entry.logger.info("第8部分：企业严重违法失信数量-当月执行完成")

    # #工商局变更数量-当月
    # 通过bbd_qyxx_id匹配dw.qyxx_bgxx_merge_clean数据；筛选change_date为当月的数据条数
    def get_gsbgsl_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            sum(case when b.bbd_qyxx_id is not null then 1 else 0 end) as gsbgsl_dy
            from all_company a 
            left join  
            qyxx_bgxx_merge_clean b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (change_date,1,7) = '{mon}'
            group by a.bbd_qyxx_id 
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'gsbgsl_dy'))

        entry.logger.info("第9部分：工商局变更数据-当月执行完成")

    # #企业收到行政许可次数
    def get_xzxk_num(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            sum(case when b.bbd_qyxx_id is not null then 1 else 0 end) as xzxk_num
            from all_company a 
            left join  
            qyxx_xzxk b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by a.bbd_qyxx_id 
        ''')

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'xzxk_num'))

        entry.logger.info("第10部分：企业收到行政许可次数执行完成")

    # 企业受到行政许可次数——当月
    def get_xzxk_num_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            sum(case when b.bbd_qyxx_id is not null then 1 else 0 end) as xzxk_num_dy
            from all_company a 
            left join  
            qyxx_xzxk b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.public_date,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'xzxk_num_dy'))

        entry.logger.info("第11部分：企业受到行政许可次数-当月执行完成")

    # #企业招标数量-当月
    # 目标公司 in（company_name_invite）（招标企业名称，需要解析该字段；直接用bbd_qyxx_id
    # join表，避免做解析步骤），统计pubdate为当月，对bbd_xgxx_id去重计数
    def get_zhaobsl_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.bbd_qyxx_id is not null then b.bbd_xgxx_id end) as zhaobsl_dy
            from all_company a 
            left join
            manage_tender b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.pubdate,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'zhaobsl_dy'))

        entry.logger.info("第12部分：企业招投标数量-当月执行完成")

    # #企业中标数量-当月
    def get_zhongbsl_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.bbd_qyxx_id is not null then b.bbd_xgxx_id end) as zhongbsl_dy
            from all_company a 
            left join
            manage_bidwinning b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.pubdate,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'zhongbsl_dy'))
        entry.logger.info("第13部分：企业中标数量执行完成")

    # #企业经营异常数量-当月
    def get_qy_jyyc_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            sum(case when b.bbd_qyxx_id is not null then 1 else 0 end) as qy_jyyc_dy
            from all_company a 
            left join
            qyxx_jyyc b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.rank_date,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_jyyc_dy'))

        entry.logger.info("第14部分：企业经营异常数量执行完成")

    # #企业欠税数量--当月
    def get_qy_qs_num_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
                select 
                a.bbd_qyxx_id,
                sum(case when b.bbd_qyxx_id is not null then 1 else 0 end) as qy_qs_num_dy
                from all_company a 
                left join
                risk_tax_owed b 
                on a.bbd_qyxx_id = b.bbd_qyxx_id
                and substring (b.announce_time,1,7) = '{mon}'
                group by a.bbd_qyxx_id
            '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_qs_num_dy'))

        entry.logger.info("第15部分：企业欠税数量执行完成")

    # #企业欠税金额--当月
    def get_qy_qs_money_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
                select 
                a.bbd_qyxx_id,
                sum(tax_money) as qy_qs_money_dy 
                from all_company a 
                left join
                risk_tax_owed b 
                on a.bbd_qyxx_id = b.bbd_qyxx_id
                and substring (b.announce_time,1,7) = '{mon}'
                group by a.bbd_qyxx_id
            '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_qs_money_dy'))

        entry.logger.info("第16部分：企业欠税金额-当月执行完成")

    # #企业股权出质量数-当月 ??public_date没有
    def get_qy_gqcz_num_dy(self, entry: Entry):
        rdd1 = entry.spark.sql(''' 
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.bbd_qyxx_id is not null then b.morregcno end) as qy_gqcz_num_dy
            from all_company a 
            left join
            qyxx_sharesimpawn b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.public_date,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_gqcz_num_dy'))

        entry.logger.info("第17部分：企业股权出质量数执行完成")

    # #企业专利数量-当月
    def get_qy_zl_num_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.bbd_qyxx_id is not null then bbd_xgxx_id end) as qy_zl_num_dy
            from all_company a 
            left join
            qyxx_wanfang_zhuanli b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.publidate,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_zl_num_dy'))

        entry.logger.info("第18部分：企业专利数量执行完成")

    # #企业软著数量-当月
    def get_qy_rz_num_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.bbd_qyxx_id is not null then bbd_xgxx_id end) as qy_rz_num_dy
            from all_company a 
            left join
            rjzzq b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.date_first_publication,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_rz_num_dy'))
        entry.logger.info("第19部分：企业软著数量-当月执行完成")

    # #企业作品著作数量
    def get_qy_zpzz_num(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.bbd_qyxx_id is not null then bbd_xgxx_id end) as qy_zpzz_num
            from all_company a 
            left join
            zpzzq b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by a.bbd_qyxx_id
        ''')

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_zpzz_num'))

        entry.logger.info("第20部分：企业著作数量执行完成")

    # #企业作品著作数量-当月
    def get_qy_zpzz_num_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.bbd_qyxx_id is not null then b.bbd_xgxx_id end) as qy_zpzz_num_dy
            from all_company a 
            left join
            zpzzq b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.creation_completion_date,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_zpzz_num_dy'))

        entry.logger.info("第21部分：企业著作数量-当月执行完成")

    # #企业商标数量-当月
    def get_qy_sb_num_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.bbd_qyxx_id is not null then b.bbd_xgxx_id end) as qy_sb_num_dy
            from all_company a 
            left join
            xgxx_shangbiao b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.reg_notice_date,1,7) = '{mon}'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_sb_num_dy'))

        entry.logger.info("第22部分：企业商标数量-当月执行完成")

    # #企业大学招聘数量-当月
    def get_qy_dxzp_num_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            sum(b.bbd_recruit_num) as qy_dxzp_num_dy
            from all_company a 
            left join
            recruit b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.pubdate,1,7) = '{mon}'
            and b.education_required rlike '大学|本科|全日制'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_dxzp_num_dy'))

        entry.logger.info("第23部分：企业招聘大学生数量-当月执行完成")

    # #企业硕士及以上招聘数量-当月
    def get_qy_ssjyszp_num_dy(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            sum(b.bbd_recruit_num) as qy_ssjyszp_num_dy
            from all_company a 
            left join
            recruit b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and substring (b.pubdate,1,7) = '{mon}'
            and b.education_required rlike '硕士|博士|博士后|海归'
            group by a.bbd_qyxx_id
        '''.format(mon=self.mon))

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_ssjyszp_num_dy'))
        entry.logger.info("第24部分：企业招聘研究生以上学历数量-当月执行完成")

    # #企业域名数量
    def get_qy_ym_num(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            count(distinct case when b.organizer_name is not null then record_license end) as qy_ym_num
            from all_company a 
            left join
            prop_domain_website b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by a.bbd_qyxx_id
        ''')

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_ym_num'))
        entry.logger.info("第25部分：企业域名数量执行完成")

    # #企业经营范围
    def get_qy_jyfw(self, entry: Entry):
        rdd1 = entry.spark.sql('''
            select 
            a.bbd_qyxx_id,
            b.operate_scope as qy_jyfw
            from all_company a
            left join   
            qyxx_basic b
            on a.bbd_qyxx_id = b.bbd_qyxx_id  
            group by 
            a.bbd_qyxx_id,
            b.operate_scope
        ''')

        save_text_to_hdfs(df=rdd1, repartition_number=10, path=os.path.join(self.savepath, 'qy_jyfw'))
        entry.logger.info('执行完成')


def main(entry: Entry):
    spark: SparkSession = entry.spark
    # 项目共有配置
    # global hive_table
    # hive_table = entry.cfg_mgr.get("hive", "database")
    # entry.logger.info(f"配置信息hive_db:{hive_table}")
    # global savepath
    # savepath = entry.cfg_mgr.hdfs.get_result_path("zhly", "index_hdfs_path")
    # global mon
    # mon = DateUtils.now2str(fmt='%Y-%m')
    # global premon
    # premon = DateUtils.add_date_4str(mon, in_fmt='%Y-%m', months=-1)
    # premon_last_day = datetime.date(datetime.date.today().year,datetime.date.today().month,1)-datetime.timedelta(1)

    company_path = entry.cfg_mgr.hdfs.get_input_path("zhly", "ly_company_path")
    # 获取楼宇企业名录
    spark.read.parquet(company_path).where("nvl(bbd_qyxx_id,'') <> '' ").cache().createOrReplaceTempView('all_company')

    S = SingerCount(entry=entry)
    # 初始化hive表
    hive_table_init(spark, tablename='qyxx_basic', new_table='qyxx_basic_pre', dateNo=S.premon)
    hive_table_init(spark, tablename='qyxx_basic')
    hive_table_init(spark, tablename='news_public_sentiment')
    hive_table_init(spark, tablename='legal_adjudicative_documents')
    hive_table_init(spark, tablename='ktgg')
    hive_table_init(spark, tablename='rjzzq')
    hive_table_init(spark, tablename='qyxx_jyyc')
    hive_table_init(spark, tablename='legal_persons_subject_to_enforcement')
    hive_table_init(spark, tablename='qyxx_yzwf')
    hive_table_init(spark, tablename='qyxx_bgxx_merge_clean')
    hive_table_init(spark, tablename='qyxx_xzxk')
    hive_table_init(spark, tablename='manage_tender')
    hive_table_init(spark, tablename='manage_bidwinning')
    hive_table_init(spark, tablename='risk_tax_owed')
    hive_table_init(spark, tablename='qyxx_sharesimpawn')
    hive_table_init(spark, tablename='qyxx_wanfang_zhuanli')
    hive_table_init(spark, tablename='zpzzq')
    hive_table_init(spark, tablename='xgxx_shangbiao')
    hive_table_init(spark, tablename='recruit')
    hive_table_init(spark, tablename='prop_domain_website')

    S.get_bdq_qy(entry)
    S.get_bdh_qy(entry)
    S.get_fmyqyd(entry)
    S.get_cpws_dy(entry)
    S.get_qy_ktgg_dy(entry)
    S.get_qy_sxbzxr_rate_dy(entry)
    S.get_qy_yzwfsx_num(entry)
    S.get_qy_yzwfsx_num_dy(entry)
    S.get_gsbgsl_dy(entry)
    S.get_xzxk_num_dy(entry)
    S.get_xzxk_num(entry)
    S.get_zhaobsl_dy(entry)
    S.get_zhongbsl_dy(entry)
    S.get_qy_jyyc_dy(entry)
    S.get_qy_qs_num_dy(entry)
    S.get_qy_qs_money_dy(entry)
    S.get_qy_gqcz_num_dy(entry)
    S.get_qy_zl_num_dy(entry)
    S.get_qy_rz_num_dy(entry)
    S.get_qy_zpzz_num(entry)
    S.get_qy_zpzz_num_dy(entry)
    S.get_qy_sb_num_dy(entry)
    S.get_qy_dxzp_num_dy(entry)
    S.get_qy_ssjyszp_num_dy(entry)
    S.get_qy_ym_num(entry)
    S.get_qy_jyfw(entry)

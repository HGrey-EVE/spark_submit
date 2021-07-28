#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 产业划分
:Author: weifuwan@bbdservice.com
:Date: 2021-02-03 16:08
"""
import datetime

from pyspark.sql import SparkSession, DataFrame
from whetstone.core.entry import Entry
from cqxdcy.proj_common.hive_util import HiveUtil
from pyspark.sql.types import StringType
from pyspark.sql import functions as fun
from cqxdcy.proj_common.date_util import DateUtils
from cqxdcy.proj_common.log_track_util import LogTrack as T




class CnUtils:

    """
    源数据路径
    """
    @classmethod
    def get_source_path(cls, entry):
        return entry.cfg_mgr.hdfs.get_input_path("hdfs", "hdfs_path")

    """
    产业映射
    """
    @classmethod
    def get_industry_mappint(cls):
        industry_dict = {
            u'集成电路': u"新一代信息技术",
            u'云计算': u"新一代信息技术",
            u'物联网': u"新一代信息技术",
            u'新一代通信网络': u"新一代信息技术",
            u'光伏': u"新能源",
            u'核电': u"新能源",
            u"风电": u"新能源",
            u'氢能': u"新能源",
            u'特种金属功能材料': u"新材料",
            u'新型无机非金属材料': u"新材料",
            u'高端金属结构材料': u"新材料",
            u'高性能复合材料': u"新材料",
            u'先进高分子材料': u"新材料",
            u'前沿新材料': u"新材料"
        }
        return industry_dict

class IndustrySplit:

    @staticmethod
    def get_type_weight(sub_industry):
        industry_dict :dict = CnUtils.get_industry_mappint()
        return industry_dict[sub_industry]

    @staticmethod
    @T.log_track
    def exec_sql(sql: str, entry: Entry):
        entry.logger.info(sql)
        entry.logger.info(f"{entry.version[:6]} ==== date")
        return entry.spark.sql(sql)


    @classmethod
    @T.log_track
    def industry_split(cls, entry: Entry):
        _sql = """
                    select *,
                    case when operate_scope rlike '单晶片|多晶硅|外延片|单晶棒|芯片|感光树脂|集成电路|半导体|晶圆|晶体' then '集成电路'
                         when operate_scope rlike '数据库|云计算|云主机|虚拟主机|云软件|云平台|云应用' then '云计算'
                         when operate_scope rlike '传感器|执行器|RFID|二维码|物联网' then '物联网'
                         when operate_scope rlike '网络测试|通信&制造|通信&终端|通信&集成|通信&应用软件|通讯&制造|通讯&终端|通讯&集成|通讯&应用软件|电信业务|5G' then '新一代通信网络'
                         when operate_scope rlike '氢能|制氢|液氢|氢储运|高压储运|固态储运|有机液态储运|氢燃料|氢供热|加氢站' then '氢能'
                         when operate_scope rlike '电站设计|核原料|核电设备|核废物处理|控制棒及驱动机构|一回路系统|反应堆|稳压器|主冷却剂泵|蒸汽发生器|给水泵|汽轮发电机|风机制冷' then '核电'
                         when operate_scope rlike '电池片组件|电池片|背板|EVA|焊带|光伏玻璃|逆变器|汇流箱|EPC|金属硅|多晶硅|硅片|银浆|PET基膜|氟模|背板|边框|接线盒|密封胶组件|压层件' then '光伏'
                         when operate_scope rlike '叶片材料|主机部件|塔架|叶片|风电主机|风电场运营|结构胶|夹层材料|玻璃纤维|碳纤维|轮毂|齿轮箱' then '风电'
                         when operate_scope rlike '纳米材料|生物材料|智能材料|超导材料' then '前沿新材料'
                         when operate_scope rlike '树脂基复合材料|碳复合材料|金属基复合材料|纤维增强复合材料|夹层复合材料|细粒复合材料|混杂复合材料|结构复合材料|功能复合材料' then '高性能复合材料'
                         when operate_scope rlike '特种橡胶|工程塑料|功能高分子材料|机硅材料|高性能氟材料|功能性膜材料|特种合成纤维' then '先进高分子材料'
                         when operate_scope rlike '先进陶瓷|特种玻璃|特种无机非金属材料|压电材料|磁性材料|导体陶瓷|激光材料|光导纤维|超硬材料|高温结构陶瓷|生物陶瓷|人造骨头|人造血管' then '新型无机非金属材料'
                         when operate_scope rlike '稀土|永磁|储氢|催化|高纯稀有金属靶材|钨钼|铼|电子级多晶硅|大尺寸单晶硅|抛光片|外延片|半导体材料|光伏薄膜|新型金属功能材料' then '特种金属功能材料'
                         when operate_scope rlike '特钢|轻合金|高强汽车钢|特种合金|新型金属结构材料' then '高端金属结构材料'
                    end sub_industry
                    from (select * from dw.qyxx_basic
                          where operate_scope rlike '网络测试|通信&制造|通信&终端|通信&集成|通信&应用软件|通讯&制造|通讯&终端|通讯&集成|通讯&应用软件|电信业务|5G|传感器|执行器|RFID|二维码|物联网
                            |单晶片|多晶硅|外延片|单晶棒|芯片|感光树脂|集成电路|半导体|晶圆|晶体
                            |数据库|云计算|云主机|虚拟主机|云软件|云平台|云应用
                            |稀土|永磁|储氢|催化|高纯稀有金属靶材|钨钼|铼|电子级多晶硅|大尺寸单晶硅|抛光片|外延片|半导体材料|光伏薄膜|新型金属功能材料
                            |先进陶瓷|特种玻璃|特种无机非金属材料|压电材料|磁性材料|导体陶瓷|激光材料|光导纤维|超硬材料|高温结构陶瓷|生物陶瓷|人造骨头|人造血管
                            |特钢|轻合金|高强汽车钢|特种合金|新型金属结构材料
                            |树脂基复合材料|碳复合材料|金属基复合材料|纤维增强复合材料|夹层复合材料|细粒复合材料|混杂复合材料|结构复合材料|功能复合材料
                            |特种橡胶|工程塑料|功能高分子材料|机硅材料|高性能氟材料|功能性膜材料|特种合成纤维
                            |纳米材料|生物材料|智能材料|超导材料
                            |电池片组件|电池片|背板|EVA|焊带|光伏玻璃|逆变器|汇流箱|EPC|金属硅|多晶硅|硅片|银浆|PET基膜|氟模|背板|边框|接线盒|密封胶组件|压层件
                            |电站设计|核原料|核电设备|核废物处理|控制棒及驱动机构|一回路系统|反应堆|稳压器|主冷却剂泵|蒸汽发生器|给水泵|汽轮发电机|风机制冷
                            |叶片材料|主机部件|塔架|叶片|风电主机|风电场运营|结构胶|夹层材料|玻璃纤维|碳纤维|轮毂|齿轮箱
                            |氢能|制氢|液氢|氢储运|高压储运|固态储运|有机液态储运|氢燃料|氢供热|加氢站'
                          and dt='{table_dt}')
                """.format(table_dt=HiveUtil.newest_partition_by_month(entry.spark, 'dw.qyxx_basic', in_month_str=entry.version[:6]))
        tab = cls.exec_sql(_sql, entry).where('sub_industry is not null')
        get_type_weight_udf = fun.udf(cls.get_type_weight, StringType())
        industry_df:DataFrame = tab.withColumn(
                'industry', get_type_weight_udf('sub_industry'))

        # 需要将 /user/cqxdcyfzyjy/temp/industry/industry.csv 中的企业从新覆盖打标签
        entry.spark.read.csv("/user/cqxdcyfzyjy/temp/industry/industry.csv", sep='`',header=True).dropDuplicates(['bbd_qyxx_id']).createOrReplaceTempView("tmp")
        # i_df = entry.spark.read.parquet("/user/cqxdcyfzyjy/tmp/industry/split")

        industry_df.dropDuplicates(['bbd_qyxx_id']).createOrReplaceTempView("tmp2")
        entry.spark.sql("""
        select
            a.*,
            b.industry as industry_new, 
            b.sub_industry as sub_industry_new
        from tmp2 a
        left join tmp b
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        """).createOrReplaceTempView("tmp3")

        entry.spark.sql("""
                select
                    regno_or_creditcode,
                    credit_code,
                    regno,
                    company_name,
                    company_type,
                    frname,
                    frname_compid,
                    frname_id,
                    regcap,
                    realcap,
                    esdate,
                    approval_date,
                    openfrom,
                    opento,
                    address,
                    operate_scope,
                    regorg,
                    enterprise_status,
                    revoke_date,
                    parent_firm,
                    regcapcur,
                    cancel_date,
                    operating_period,
                    form,
                    bbd_qyxx_id,
                    bbd_history_name,
                    company_industry,
                    regcap_amount,
                    regcap_currency,
                    realcap_amount,
                    realcap_currency,
                    company_currency,
                    company_province,
                    company_enterprise_status,
                    company_companytype,
                    company_regorg,
                    ipo_company,
                    company_county,
                    type,
                    bbd_type,
                    bbd_dotime,
                    bbd_uptime,
                    ctime,
                    history_name,
                    frname_cerno,
                    frname_check_id,
                    frname_group_id,
                    revoke_reason,
                    company_gis_lat,
                    company_gis_lon,
                    bbdgis_dotime,
                    dt,
                    case when nvl(industry_new,'') == ''  then industry
                    else industry_new end as industry,
                    case when nvl(sub_industry_new,'') == ''  then sub_industry
                    else sub_industry_new end as sub_industry
                from tmp3
        """).createOrReplaceTempView("tmp4")

        entry.spark.sql("select a.* from tmp a inner join tmp2 b on a.bbd_qyxx_id = b.bbd_qyxx_id").createOrReplaceTempView(
            "tmp10")
        entry.spark.sql(
            "select * from tmp a where not exists (select 1 from tmp10 b where a.bbd_qyxx_id = b.bbd_qyxx_id)").createOrReplaceTempView(
            "tmp11")

        entry.spark.sql("""
        select
            a.regno_or_creditcode,
            a.credit_code,
            a.regno,
            a.company_name,
            a.company_type,
            a.frname,
            a.frname_compid,
            a.frname_id,
            a.regcap,
            a.realcap,
            a.esdate,
            a.approval_date,
            a.openfrom,
            a.opento,
            a.address,
            a.operate_scope,
            a.regorg,
            a.enterprise_status,
            a.revoke_date,
            a.parent_firm,
            a.regcapcur,
            a.cancel_date,
            a.operating_period,
            a.form,
            a.bbd_qyxx_id,
            a.bbd_history_name,
            a.company_industry,
            a.regcap_amount,
            a.regcap_currency,
            a.realcap_amount,
            a.realcap_currency,
            a.company_currency,
            a.company_province,
            a.company_enterprise_status,
            a.company_companytype,
            a.company_regorg,
            a.ipo_company,
            a.company_county,
            a.type,
            a.bbd_type,
            a.bbd_dotime,
            a.bbd_uptime,
            a.ctime,
            a.history_name,
            a.frname_cerno,
            a.frname_check_id,
            a.frname_group_id,
            a.revoke_reason,
            a.company_gis_lat,
            a.company_gis_lon,
            a.bbdgis_dotime,
            a.dt,
            b.industry,
            b.sub_industry
        from tmp11 b
        inner join (select * from dw.qyxx_basic where dt = '{table_dt}') a
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        """.format(table_dt=HiveUtil.newest_partition_by_month(entry.spark, 'dw.qyxx_basic', in_month_str=entry.version[:6])))\
            .dropDuplicates(["bbd_qyxx_id"]).createOrReplaceTempView("tmp5")

        # 将匹配上的数据和线下提供的数据进行union起来
        entry.spark.sql("""
            select
                regno_or_creditcode,
                    credit_code,
                    regno,
                    company_name,
                    company_type,
                    frname,
                    frname_compid,
                    frname_id,
                    regcap,
                    realcap,
                    esdate,
                    approval_date,
                    openfrom,
                    opento,
                    address,
                    operate_scope,
                    regorg,
                    enterprise_status,
                    revoke_date,
                    parent_firm,
                    regcapcur,
                    cancel_date,
                    operating_period,
                    form,
                    bbd_qyxx_id,
                    bbd_history_name,
                    company_industry,
                    regcap_amount,
                    regcap_currency,
                    realcap_amount,
                    realcap_currency,
                    company_currency,
                    company_province,
                    company_enterprise_status,
                    company_companytype,
                    company_regorg,
                    ipo_company,
                    company_county,
                    type,
                    bbd_type,
                    bbd_dotime,
                    bbd_uptime,
                    ctime,
                    history_name,
                    frname_cerno,
                    frname_check_id,
                    frname_group_id,
                    revoke_reason,
                    company_gis_lat,
                    company_gis_lon,
                    bbdgis_dotime,
                    dt,
                    industry,
                    sub_industry
            from tmp4
            union all
            select
                regno_or_creditcode,
                    credit_code,
                    regno,
                    company_name,
                    company_type,
                    frname,
                    frname_compid,
                    frname_id,
                    regcap,
                    realcap,
                    esdate,
                    approval_date,
                    openfrom,
                    opento,
                    address,
                    operate_scope,
                    regorg,
                    enterprise_status,
                    revoke_date,
                    parent_firm,
                    regcapcur,
                    cancel_date,
                    operating_period,
                    form,
                    bbd_qyxx_id,
                    bbd_history_name,
                    company_industry,
                    regcap_amount,
                    regcap_currency,
                    realcap_amount,
                    realcap_currency,
                    company_currency,
                    company_province,
                    company_enterprise_status,
                    company_companytype,
                    company_regorg,
                    ipo_company,
                    company_county,
                    type,
                    bbd_type,
                    bbd_dotime,
                    bbd_uptime,
                    ctime,
                    history_name,
                    frname_cerno,
                    frname_check_id,
                    frname_group_id,
                    revoke_reason,
                    company_gis_lat,
                    company_gis_lon,
                    bbdgis_dotime,
                    dt,
                    industry,
                    sub_industry
            from tmp5
        """).repartition(20)\
            .write\
            .parquet(f"{CnUtils.get_source_path(entry)}/industry_divide", mode='overwrite')



def pre_check(entry: Entry):
    return True

def main(entry: Entry):

    ## 企业划分
    IndustrySplit.industry_split(entry)


def post_check(entry: Entry):
    return True

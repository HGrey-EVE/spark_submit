#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-02-03 16:08
"""

import os
import json
from whetstone.core.entry import Entry
from pyspark.sql import SparkSession, DataFrame, Row
from cqxdcy.proj_common.hive_util import HiveUtil
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType


def pre_check(entry: Entry):
    return True


class CnUtils:
    """
    原始路径
    """

    @classmethod
    def get_source_path(cls, entry) -> str:
        return entry.cfg_mgr.hdfs.get_input_path("hdfs", "hdfs_path")

    """
    输出路径
    """

    @classmethod
    def get_final_path(cls, entry) -> str:
        return entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_path")

    """
    临时路径
    """

    @classmethod
    def get_temp_path(cls, entry) -> str:
        return entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path")


def zz_to_json_array(zz):
    if zz == 'null' or zz is None:
        return '[]'
    return json.dumps(zz.split(','), ensure_ascii=False)


def str_to_json_array(s):
    return json.dumps([s], ensure_ascii=False)


# 招商价值分详情
def zsjzdexq(nsnl_score, zhss_3years_score, zhsl_score, zz_score,
             company_zzzl_score, company_zzsl_score, create_ability_score,
             sb_num_score, rz_num_score, zl_score, sp_score, rcgx_score,
             yq_num_score, yq_score, rylxx_score, gxlbl_score):
    data = {
        "纳税能力": {
            "socre": nsnl_score,
            "detail": {
                "综合税收（前3年）": zhss_3years_score,
                "综合税率": zhsl_score
            }
        },
        "资质得分": {
            "socre": zz_score,
            "detail": {
                "企业获得资质的质量": company_zzzl_score,
                "企业获得资质的数量": company_zzsl_score
            }
        },
        "创新能力": {
            "socre": create_ability_score,
            "detail": {
                "企业的商标": sb_num_score,
                "软件著作": rz_num_score,
                "专利": zl_score,
                "授牌": sp_score
            }
        },
        "人才贡献": {
            "socre": rcgx_score,
            "detail": {
                "高学历比例": gxlbl_score
            }
        },
        "舆情影响": {
            "socre": yq_score,
            "detail": {
                "舆情总数": yq_num_score,
                "荣誉类信息数据": rylxx_score
            }
        }
    }
    return json.dumps(data, ensure_ascii=False)


def zskxxxq(hyqspg_score, tdnl_score, sccykj_score, hyjz_score, ysxfx_score,
            qybj_score, khsc_score, nlxpg_score, ylnl_score, yfnl_score,
            qyxwx_score, tzph_score, tzhyd_score):
    data = {
        "行业趋势评估": {
            "socre": hyqspg_score,
            "detail": {
                "产业市场空间": sccykj_score,
                "行业竞争": hyjz_score
            }
        },
        "约束性分析": {
            "socre": ysxfx_score,
            "detail": {
                "区域布局": qybj_score,
                "客户市场": khsc_score,
            }
        },
        "能力性评估": {
            "socre": nlxpg_score,
            "detail": {
                "盈利能力": ylnl_score,
                "研发能力": yfnl_score,
                "团队能力": tdnl_score
            }
        },
        "企业行为性指标": {
            "socre": qyxwx_score,
            "detail": {
                "投资偏好": tzph_score,
                "投资活跃度": tzhyd_score
            }
        }
    }
    return json.dumps(data, ensure_ascii=False)


def get_company_county(spark: SparkSession):
    """
    获取省市区
    :return: DataFrame
    """
    dt = HiveUtil.newest_partition(spark, 'dw.company_county')
    df = spark.sql(f"""
                select company_county, province, city, region
                from 
                    (select code as company_county, province, city, district as region, 
                                (row_number() over(partition by code order by tag )) as row_num
                    from dw.company_county 
                    where dt = '{dt}' and tag != -1) b 
                where row_num = 1
            """)
    return df


def cal_province_city_region(entry: Entry):
    """
    计算公司省 市 区
    :return: DataFrame
    """
    spark = entry.spark
    path = os.path.join(CnUtils.get_source_path(entry), 'industry_divide')
    spark.read.parquet(path).dropDuplicates(['bbd_qyxx_id']).createOrReplaceTempView('qyxx_basic')

    get_company_county(spark).createOrReplaceTempView('company_country')
    sql = """
            select 
                R.*,
                L.province,
                L.city,
                L.region 
            from company_country L 
            right join qyxx_basic R 
            on L.company_county = R.company_county
    """
    company_county_df = spark.sql(sql)

    return company_county_df


def get_qy_guimo(spark: SparkSession):
    """
    获取企业规模数据
    :return: DataFrame
    """
    dt = HiveUtil.newest_partition(spark, 'dw.enterprisescaleout')
    # dt = '20200501'
    sql = f"""
            select 
                bbd_qyxx_id,
                company_scale 
            from dw.enterprisescaleout 
            where dt ='{dt}' 
            and nvl(bbd_qyxx_id, '') != ''
        """
    get_qy_guimo_df = spark.sql(sql).dropDuplicates(['bbd_qyxx_id'])
    print("获取企业规模")
    return get_qy_guimo_df


def zhongweishu(test):
    if len(test) % 2 == 0:
        mid = (sorted(test)[int(len(test) / 2)] + sorted(test)[int(len(test) / 2) + 1]) / 2
    # 奇数
    else:
        mid = sorted(test)[int(len(test) / 2)]
    return mid


def to_push_data(row: Row):
    """
    返回订阅推送结构的数据
    :param row:
    :return:
    """
    tmp_data = row.asDict(True)
    print(tmp_data)
    push_data = {}
    tn = "cq_xdcy_merchant_investment_trend"
    push_data["tn"] = tn
    # push_data["data"] = {tn: [tmp_data]}
    push_data["data"] = tmp_data
    return push_data


def to_push_data2(row: Row):
    """
    返回订阅推送结构的数据
    :param row:
    :return:
    """
    tmp_data = row.asDict(True)
    print(tmp_data)
    push_data = {}
    tn = "cq_xdcy_merchant_enterprise_list"
    push_data["tn"] = tn
    # push_data["data"] = {tn: [tmp_data]}
    push_data["data"] = tmp_data
    return push_data


schema = StructType([
    StructField("tn", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True)])


def register_udf(entry: Entry):
    spark = entry.spark
    spark.udf.register("zz_to_json_array", zz_to_json_array)
    spark.udf.register("str_to_json_array", str_to_json_array)
    spark.udf.register("zsjzdexq", zsjzdexq)
    spark.udf.register("zskxxxq", zskxxxq)


def basic_data_cal(entry: Entry):
    spark = entry.spark
    dt = entry.version
    spark.read.csv(f"/user/cqxdcyfzyjy/{dt}/tmp/cqxdcy/model/zsjz_result.csv", header=True,
                   sep=',').createOrReplaceTempView("zsjz")
    spark.read.csv(f"/user/cqxdcyfzyjy/{dt}/tmp/cqxdcy/model/zskxx_result.csv", header=True,
                   sep=',').createOrReplaceTempView("zskxx_result")
    cal_province_city_region(entry).cache().createOrReplaceTempView("basic_tmp")
    spark.sql("""
        select
            a.bbd_qyxx_id,
            a.zhss_3years_score,
            a.zhsl_score,
            a.company_zzzl_score,
            a.company_zzsl_score,
            a.sb_num_score,
            a.rz_num_score,
            a.zl_score,
            a.sp_score,
            a.yq_num_score,
            a.rylxx_score,
            a.ns_ability_score,
            a.zz_score,
            a.create_ability_score,
            a.rcgx_score,
            a.yq_score,
            a.zsjz_score,
            b.industry,
            b.sccykj_score,
            b.hyjz_score,
            b.qybj_score,
            b.khsc_score,
            b.ylnl_score,
            b.nsnl_score,
            b.yfnl_score,
            b.tdnl_score,
            b.tzph_score,
            b.tzhyd_score,
            b.hyqspg_score,
            b.ysxfx_score,
            b.nlxpg_score,
            b.qyxwx_score,
            b.zskxx_score,
            a.gxlbl_score
        from
            zsjz a
            join zskxx_result b on a.bbd_qyxx_id = b.bbd_qyxx_id
        """).cache().createOrReplaceTempView("basic_data")


def enterprise_list_cal(entry: Entry):
    spark = entry.spark
    dt = entry.version
    cq_xdcy_merchant_enterprise_list_df = spark.sql(f"""
       select
           industry,
           company_industry,
           sub_industry,
           province,
           area,
           city,
           company_name,
           id,
           bbd_qyxx_id,
           establish_age,
           esdate,
           qualification,
           regcap,
           regcap_amount, 
           address,
           frname,
           comprehensive_value,
           investment_value,
           value_detail,
           investment_feasibility,
           feasibility_detail,
           date,
           case when rank <= 1000  then '龙头企业'
                when rank between 1001 and 10000 then '骨干企业'
                else '成长性企业' end as label,
           update_time
       from (
       select
           *,
           row_number() over(partition by industry order by comprehensive_value desc ) rank
       from (
       select 
           str_to_json_array(a.industry) as industry,
           a.industry as company_industry,
           str_to_json_array(c.sub_industry) as sub_industry,
           c.province,
           c.region as area,
           c.city,
           c.company_name,
           concat(c.bbd_qyxx_id,c.dt) as id,
           a.bbd_qyxx_id,
           year(current_date)-year(c.esdate)  as establish_age,
           cast(c.esdate as string) as esdate,
           zz_to_json_array(b.nick_name) as qualification,
           c.regcap,
           c.regcap_amount, 
           c.address,
           c.frname,
           (a.zsjz_score + a.zskxx_score)/2.0 as comprehensive_value,
           a.zsjz_score  as investment_value,
           zsjzdexq(a.nsnl_score,a.zhss_3years_score,a.zhsl_score,a.zz_score,a.company_zzzl_score,a.company_zzsl_score,a.create_ability_score,a.sb_num_score,a.rz_num_score,a.zl_score,a.sp_score,a.rcgx_score,a.yq_num_score,a.yq_score,a.rylxx_score,a.gxlbl_score) as value_detail,
           a.zskxx_score  as investment_feasibility,
           zskxxxq(hyqspg_score,a.tdnl_score,a.sccykj_score,a.hyjz_score,a.ysxfx_score,a.qybj_score,a.khsc_score,a.nlxpg_score,a.ylnl_score,a.yfnl_score,a.qyxwx_score,a.tzph_score,a.tzhyd_score) as feasibility_detail,
           '{entry.version[0:4]}-{entry.version[4:6]}-{entry.version[6:8]}' as date,
           cast(current_date as string) as update_time
       from
           basic_data a
       left join 
           dw.news_cqzstz b on a.bbd_qyxx_id = b.bbd_qyxx_id
       left join 
           basic_tmp c on a.bbd_qyxx_id = c.bbd_qyxx_id
       ) a
       ) b
       """)

    guimo = get_qy_guimo(spark)
    cq_xdcy_merchant_enterprise_list_df = cq_xdcy_merchant_enterprise_list_df.join(guimo, 'bbd_qyxx_id', 'left') \
        .fillna('', subset=['company_scale']).dropDuplicates(['bbd_qyxx_id'])
    cq_xdcy_merchant_enterprise_list_df.write.parquet(
        f"/user/cqxdcyfzyjy/model_result/{dt}/cq_xdcy_merchant/cq_xdcy_merchant_enterprise_list", mode='overwrite')
    schema = StructType([
        StructField("tn", StringType(), True),
        StructField("data", MapType(StringType(), StringType()), True)])

    df = spark.read.parquet(f"/user/cqxdcyfzyjy/model_result/{dt}/cq_xdcy_merchant/cq_xdcy_merchant_enterprise_list")
    spark.createDataFrame(df.rdd.map(to_push_data2), schema).repartition(20).write.json(
        f"{CnUtils.get_final_path(entry)}/cq_xdcy_merchant_enterprise_list", mode="overwrite")


def investment_trend_cal(entry: Entry):
    spark = entry.spark
    dt = entry.version
    dfl = spark.sql(
        """
        select industry, zsjz_score 
        from zsjz
        """
    ).cache()
    rdd1 = dfl.rdd.groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x: (x[0], list(map(float, x[1])))).map(
        lambda x: (x[0], zhongweishu(x[1])))

    schema = StructType([StructField("industry", StringType(), True), StructField("total_zhong", StringType(), True)])
    spark.createDataFrame(rdd1, schema).createOrReplaceTempView("szjz_zws")

    dfl = spark.sql(
        """
        select industry,zskxx_score 
        from zskxx_result
        """
    ).cache()
    rdd1 = dfl.rdd.groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x: (x[0], list(map(float, x[1])))).map(
        lambda x: (x[0], zhongweishu(x[1])))

    schema = StructType([StructField("industry", StringType(), True), StructField("total_zhong", StringType(), True)])
    spark.createDataFrame(rdd1, schema).createOrReplaceTempView("sskxx_zws")

    cq_xdcy_merchant_investment_trend_df = spark.sql(f"""
            select
                a.bbd_qyxx_id,
                concat(c.bbd_qyxx_id,c.dt) as id,
                a.zhsl_score  as investment_value,
                zsjz_avg.zsjz_score_avg as industry_value_avg,
                jz.total_zhong as industry_value_median,
                a.zskxx_score as investment_feasibility,
                kxx_avg.zskxx_score_avg as industry_feasibility_avg,
                kxx.total_zhong as industry_feasibility_median,
                '{entry.version[0:4]}-{entry.version[4:6]}-{entry.version[6:8]}' as date,
                cast(current_date as string) as update_time
            from
                basic_data a
            inner join basic_tmp c on a.bbd_qyxx_id = c.bbd_qyxx_id
            left join szjz_zws jz on jz.industry = a.industry
            left join sskxx_zws kxx on kxx.industry = a.industry
            left join (
                select 
                    industry,avg(zskxx_score) as zskxx_score_avg
                from 
                    zskxx_result
                group
                    by industry
            ) kxx_avg on kxx_avg.industry = a.industry
            left join (
                select 
                    industry,avg(zsjz_score) as zsjz_score_avg
                from 
                    zsjz
                group
                    by industry
            ) zsjz_avg on zsjz_avg.industry = a.industry
        """)
    cq_xdcy_merchant_investment_trend_df.write.parquet(
        f"/user/cqxdcyfzyjy/model_result/{dt}/cq_xdcy_merchant/cq_xdcy_merchant_investment_trend", mode='overwrite')
    schema = StructType([
        StructField("tn", StringType(), True),
        StructField("data", MapType(StringType(), StringType()), True)])

    df = spark.read.parquet(f"/user/cqxdcyfzyjy/model_result/{dt}/cq_xdcy_merchant/cq_xdcy_merchant_investment_trend")
    spark.createDataFrame(df.rdd.map(to_push_data), schema).repartition(20).write.json(
        f"{CnUtils.get_final_path(entry)}/cq_xdcy_merchant_investment_trend", mode="overwrite")


def main(entry: Entry):
    register_udf(entry)
    basic_data_cal(entry)
    enterprise_list_cal(entry)
    investment_trend_cal(entry)


def post_check(entry: Entry):
    return True

#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: company_tags
:Author: zhangwensha@bbdservice.com
:Date: 2021-06-10 3:15 PM
:Version: v.1.0
:Description: 企业标签计算，由算法(张文莎)提供
"""
import os
import re

from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkConf
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as Fn
import numpy as np
import pandas as pd
import xlrd

from whetstone.core.entry import Entry
from jingxinting.enums.enum_industry import Industry
from jingxinting.proj_common.common_util import check_hdfs_path_exists
from jingxinting.proj_common.data_output_util import ResultOutputUtil
from jingxinting.proj_common.hive_util import HiveUtil


# ind_dict = {
#     '数字经济': 0,
#     '软件与信息': 1,
#     '大数据': 2,
#     '集成电路与新型显示': 3,
#     '新一代网络': 4,
#     '航空与燃机': 5,
#     '智能装备': 6,
#     '轨道交通': 7,
#     '能源与智能汽车': 8,
#     '农产品精深加工': 9,
#     '优质白酒': 10,
#     '精制川茶': 11,
#     '医药健康': 12,
#     '新材料': 13,
#     '清洁能源': 14,
#     '绿色化工': 15,
#     '节能环保': 16
# }


def read_excel(file_path):
    def seperate(lstring):
        if len(lstring) > 0 and lstring[-1] == '等':
            lstring = lstring[:-1]
        wlist = re.split(',|，|、|;|；|\n|\/|\'|\[|\]| ', lstring)
        return [x for x in wlist if len(x) > 0]

    # 获取数据
    data = xlrd.open_workbook(file_path)
    # 获取所有sheet名字
    sheet_names = data.sheet_names()
    ret = {}
    for sheet in sheet_names:
        print(sheet)
        # 获取sheet
        table = data.sheet_by_name(sheet)
        # 获取总行数
        nrows = table.nrows  # 包括标题
        # 获取总列数
        ncols = table.ncols

        # 计算出合并的单元格有哪些
        colspan = {}
        if table.merged_cells:
            for item in table.merged_cells:
                for row in range(item[0], item[1]):
                    for col in range(item[2], item[3]):
                        # 合并单元格的首格是有值的，所以在这里进行了去重
                        if (row, col) != (item[0], item[2]):
                            colspan.update({(row, col): (item[0], item[2])})

        # 获取列名
        colnames = []
        for j in range(ncols):
            # 假如碰见合并的单元格坐标，取合并的首格的值即可
            if colspan.get((0, j)):
                colnames.append(table.cell_value(*colspan.get((0, j))))
            else:
                colnames.append(table.cell_value(0, j))
        print('colnames')
        print(colnames)
        df = pd.DataFrame(columns=colnames)

        # 读取每行数据
        for i in range(1, nrows):
            row = []
            for j in range(ncols):
                # 假如碰见合并的单元格坐标，取合并的首格的值即可
                if colspan.get((i, j)):
                    row.append(seperate(table.cell_value(*colspan.get((i, j)))))
                else:
                    row.append(seperate(table.cell_value(i, j)))
            print(row)
            df.loc[i - 1] = row

        ret[sheet] = df

    for sheet in ret.keys():
        for column in ret[sheet].columns:
            if max(ret[sheet][column].apply(len)) == 1:
                ret[sheet][column] = ret[sheet][column].apply(lambda x: x[0] if len(x) > 0 else None)

    return (ret)


def get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags=[]):
    id_list1 = spark.sql('''
            select distinct bbd_qyxx_id, operate_scope as text from basic_table
            where {}
        '''.format(basic_rule))
    id_list2 = spark.sql('''
        select bbd_qyxx_id, concat_ws(',', collect_set(company_introduction)) as text
            from recruit_table
            where {}
            group by bbd_qyxx_id
        '''.format(recruit_rule))
    id_list = id_list1.union(id_list2)

    schema = StructType([
            StructField("bbd_qyxx_id", StringType(), True),
            StructField("ind_16p1", StringType(), True),
            StructField("parent_tag", StringType(), True),
            StructField("tag", StringType(), True),
            StructField("tag_level",StringType(),True)
            ])
    
    result = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    id_list.registerTempTable('id_list')
    for j in range(len(tag_list)):
        tmp = spark.sql('''
            select bbd_qyxx_id, '{}' as ind_16p1, '{}' as parent_tag, '{}' as tag, '{}' as tag_level 
            from id_list'''.format(ind_16p1,tag_list[j-1] if j!=0 else '',tag_list[j],j+1))        
        result = result.union(tmp)
    if len(newtags)>0:
        for j in range(len(newtags)):
            tmp = spark.sql('''
                select distinct bbd_qyxx_id, '{}' as ind_16p1, '{}' as parent_tag, '{}' as tag, '{}' as tag_level 
                from id_list
                where text like '%{}%' '''.format(ind_16p1,tag_list[-1],newtags[j],len(tag_list)+1,newtags[j]))   
            result = result.union(tmp)
    return(result)



def prepare_data(spark, dw_name, output_path):
    """提取四川所有存续企业的经营范围与招聘简介
    :param spark:
    :param dw_name:
    :param output_path:
    :return:
    """
    basic_dt = HiveUtil.newest_partition(spark, f'{dw_name}.qyxx_basic')
    recruit_dt = HiveUtil.newest_partition(spark, f'{dw_name}.manage_recruit')
    basic_table = spark.sql('''
           select distinct bbd_qyxx_id, company_name, operate_scope from dw.qyxx_basic
           where dt = '{}'
           and company_province = '四川'
           and company_type not like '%个体%'
           and enterprise_status not like '%吊销%'
       '''.format(basic_dt)).repartition(1000)
    basic_table.registerTempTable('basic_table')
    basic_table.write.csv(os.path.join(output_path, 'basic_table'), mode='overwrite', header=True)

    recruit_table = spark.sql('''
       select distinct a.bbd_qyxx_id, a.company_name, company_introduction
       from basic_table a join dw.manage_recruit b
       on dt = '{}'
       and a.bbd_qyxx_id = b.bbd_qyxx_id
       '''.format(recruit_dt))
    recruit_table.repartition(1000).write.csv(os.path.join(output_path, 'recruit_table'), mode='overwrite', header=True)


def get_rules(p11, p12, p2='', d='', r=''):
    basic_rule = ''' ((operate_scope rlike '{}' and operate_scope rlike '{}')
                     or
                    (operate_scope rlike '{}' and {}))
                    '''.format(p11, p12, p2, len(p2) > 0)
    if len(d) > 0:
        basic_rule = re.sub('operate_scope',
                            "regexp_replace(operate_scope,'" + d + "|\([~\(\)]\)|（[~（）]）','')", basic_rule)
    else:
        basic_rule = re.sub('operate_scope', "regexp_replace(operate_scope,'\([~\(\)]\)|（[~（）]）','')", basic_rule)
    if len(r) > 0:
        basic_rule = basic_rule + " and company_name not rlike '" + r + "'"

    recruit_rule = re.sub('operate_scope', 'company_introduction', basic_rule)
    if 'company_introduction' in r:
        recruit_rule = False
    return basic_rule, recruit_rule


def concat_rules(rlist):
    rlist = [r for r in rlist if r and len(r) > 0]
    return '|'.join(rlist)


def save_to_append_path(spark, df, write_path, dump_path):
    if not check_hdfs_path_exists(write_path):
        df.write.parquet(write_path, mode='overwrite')
        return
    spark.read.parquet(write_path).union(df).write.parquet(dump_path, mode='overwrite')
    spark.read.parquet(dump_path).write.parquet(write_path, mode='overwrite')


def calc_ind_16p1(logger, spark: SparkSession, ind_dict, output_path, keywords_path, tmp_path):
    """
    16+1产业图谱标签计算
    :param logger:
    :param spark:
    :param ind_dict: 16+1标签名字对应的标签码值
    :param output_path:
    :param keywords_path:
    :param tmp_path:
    :return:
    """

    # 读取关键词文件
    data = read_excel(keywords_path)
    logger.info("load excel success")

    basic_table = spark.read.csv(os.path.join(output_path, 'basic_table'), header=True, inferSchema=True)
    recruit_table = spark.read.csv(os.path.join(output_path, 'recruit_table'), header=True, inferSchema=True)
    basic_table.persist().take(1)
    recruit_table.persist().take(1)
    basic_table.createOrReplaceTempView('basic_table')
    recruit_table.createOrReplaceTempView('recruit_table')
    logger.info("load basic table and recruit table success")

    # 创建df存储结果
    # schema = StructType([
    #     StructField("bbd_qyxx_id", StringType(), True),
    #     StructField("ind_16p1", StringType(), True),
    #     StructField("parent_tag", StringType(), True),
    #     StructField("tag", StringType(), True),
    #     StructField("tag_level", StringType(), True)
    # ])
    # result = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    write_path = os.path.join(tmp_path, 'ret_write')
    dump_path = os.path.join(tmp_path, 'dump_path')

    # 数字经济
    sheet = '数字经济'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词']]
        p11 = concat_rules([rrow['一级关键词'], rrow['近义词']])
        p12 = rrow['二级关键词']
        p2 = rrow['新增关键词']
        d = rrow['删除词']
        r = rrow['排除词']
        basic_rule, recruit_rule = get_rules(p11, p12, p2, d, r)
        tmp = get_industry_company(spark, ind_16p1, basic_rule, recruit_rule, tag_list)
        save_to_append_path(spark, tmp, write_path, dump_path)

    spark\
        .read\
        .parquet(write_path)\
        .repartition(100)\
        .write\
        .csv(os.path.join(output_path, 'ind_16p1_companylist'), mode='overwrite', header=True)
    logger.info(f"====write {sheet} success====")

    # 1_集成电路与新型展示
    sheet = '1_集成电路与新型显示'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词'], rrow['二级关键词'], rrow['三级关键词']]
        p11 = '' if rrow['一级关键词'] == '新型显示' else '集成电路'
        p12 = concat_rules([rrow['四级关键词'], rrow['三级关键词'], rrow['新增关键词']])
        d = rrow['删除词']
        basic_rule, recruit_rule = get_rules(p11, p12, d=d)
        newtags = row['四级关键词']
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 2_软件与信息服务产业
    sheet = '2_软件与信息服务产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词'], rrow['二级关键词']]
        p11 = '软件|信息技术'
        p12 = concat_rules([rrow['二级关键词'], rrow['三级关键词']])
        basic_rule, recruit_rule = get_rules(p11, p12)
        newtags = row['三级关键词']
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)        
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 3_大数据产业
    sheet = '3_大数据产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词']]
        p11 = k
        p12 = concat_rules([rrow['二级关键词'], rrow['新增关键词'], rrow['一级关键词'], rrow['近义词']])
        basic_rule, recruit_rule = get_rules(p11, p12)
        newtags=row['二级关键词']
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 4_新一代网络技术
    sheet = '4_新一代网络技术'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词'], rrow['二级关键词']] if len(rrow['四级关键词']) == 0 else [rrow['一级关键词'], rrow['二级关键词'],
                                                                                   rrow['三级关键词']]
        p11 = rrow['一级关键词']
        p12 = concat_rules([rrow['二级关键词'], rrow['三级关键词'], rrow['新增关键词']]) if len(rrow['四级关键词']) == 0 else concat_rules(
            [rrow['三级关键词'], rrow['四级关键词'], rrow['新增关键词']])
        basic_rule, recruit_rule = get_rules(p11, p12)
        newtags = row['三级关键词'] if len(rrow['四级关键词'])==0 else []
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 5_航空与燃机产业
    sheet = '5_航空与燃机产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词']] if len(rrow['三级关键词']) == 0 else [rrow['一级关键词'], rrow['二级关键词']]
        p11 = '航空'
        p12 = concat_rules([rrow['一级关键词'], rrow['二级关键词'], rrow['新增关键词'], rrow['近义词']]) if len(
            rrow['三级关键词']) == 0 else concat_rules([rrow['二级关键词'], rrow['新增关键词'], rrow['三级关键词']])
        d = rrow['删除词']
        r = rrow['排除词']
        newtags = row['二级关键词'] if len(rrow['三级关键词'])==0 else []
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 6_智能装备产业
    sheet = '6_智能装备产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词']] if len(rrow['三级关键词']) == 0 else [rrow['一级关键词'], rrow['二级关键词']]
        p11 = ''
        p12 = concat_rules([rrow['一级关键词'], rrow['二级关键词'], rrow['新增关键词']]) if len(rrow['三级关键词']) == 0 else concat_rules(
            [rrow['二级关键词'], rrow['新增关键词'], rrow['三级关键词']])
        basic_rule, recruit_rule = get_rules(p11, p12)
        newtags = row['二级关键词'] if len(rrow['三级关键词'])==0 else []
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 7_轨道交通产业
    sheet = '7_轨道交通产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词'], rrow['二级关键词']] if len(rrow['四级关键词']) == 0 else [rrow['一级关键词'], rrow['二级关键词'],
                                                                                   rrow['三级关键词']]
        p11 = k
        p12 = concat_rules([rrow['二级关键词'], rrow['三级关键词'], rrow['新增关键词']]) if len(rrow['四级关键词']) == 0 else concat_rules(
            [rrow['三级关键词'], rrow['四级关键词'], rrow['新增关键词']])
        basic_rule, recruit_rule = get_rules(p11, p12)
        newtags = row['三级关键词'] if len(rrow['四级关键词'])==0 else []
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 8_能源与智能汽车产业
    sheet = '8_能源与智能汽车产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词']] if len(rrow['三级关键词']) == 0 else [rrow['一级关键词'], rrow['二级关键词']]
        p11 = '新能源汽车|智能汽车|智能车|自动驾驶'
        p12 = concat_rules([rrow['一级关键词'], rrow['二级关键词'], rrow['新增关键词'], rrow['近义词']]) if len(
            rrow['三级关键词']) == 0 else concat_rules([rrow['二级关键词'], rrow['新增关键词'], rrow['三级关键词']])
        r = rrow['排除词']
        basic_rule, recruit_rule = get_rules(p11, p12, r=r)
        newtags = row['二级关键词'] if len(rrow['三级关键词'])==0 else []   
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 9_农产品精深加工产业
    sheet = '9_农产品精深加工产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词']]
        p11 = '加工|生产'
        p12 = concat_rules([row['一级关键词'][0][:-2], rrow['二级关键词'], rrow['近义词']])
        basic_rule, recruit_rule = get_rules(p11, p12)
        if rrow['一级关键词'] == '化妆品加工|生产':
            basic_rule = basic_rule + " and company_name rlike '生物|生化|化妆品'"
            recruit_rule = recruit_rule + " and company_name rlike '生物|生化|化妆品'"
        tmp = get_industry_company(spark, ind_16p1, basic_rule, recruit_rule, tag_list)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 10_精制川茶产业
    sheet = '10_精制川茶产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词']] if len(rrow['三级关键词']) == 0 else [rrow['一级关键词'], rrow['二级关键词']]
        p11 = '茶'
        p12 = concat_rules([rrow['一级关键词'], rrow['二级关键词']]) if len(rrow['三级关键词']) == 0 else concat_rules(
            [rrow['二级关键词'], rrow['三级关键词']])
        basic_rule, recruit_rule = get_rules(p11, p12)
        newtags = row['二级关键词'] if len(rrow['三级关键词'])==0 else []   
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 11_优质白酒产业
    sheet = '11_优质白酒产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词'], rrow['二级关键词']] if len(rrow['四级关键词']) == 0 else [rrow['一级关键词'], rrow['二级关键词'],
                                                                                   rrow['三级关键词']]
        p11 = '白酒'
        p12 = concat_rules([rrow['二级关键词'], rrow['三级关键词'], rrow['近义词']]) if len(rrow['四级关键词']) == 0 else concat_rules(
            [rrow['三级关键词'], rrow['四级关键词'], rrow['近义词']])
        basic_rule, recruit_rule = get_rules(p11, p12)
        newtags = row['三级关键词'] if len(rrow['四级关键词'])==0 else []
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 12_医药健康产业
    sheet = '12_医药健康产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词'], rrow['二级关键词']]
        p11 = '药'
        p12 = concat_rules([rrow['三级关键词'], rrow['二级关键词']])
        r = rrow['排除词']
        basic_rule, recruit_rule = get_rules(p11, p12, r=r)
        newtags = row['三级关键词']
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 13_新材料产业
    sheet = '13_新材料产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词'], rrow['二级关键词']]
        p11 = re.sub('材料|新材料', '', rrow['一级关键词'])
        p12 = concat_rules([rrow['三级关键词'], rrow['二级关键词'], rrow['新增关键词'], rrow['近义词']])
        basic_rule, recruit_rule = get_rules(p11, p12)
        newtags = row['三级关键词']
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 14_清洁能源产业
    sheet = '14_清洁能源产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词'], rrow['二级关键词']] if len(rrow['四级关键词']) == 0 else [rrow['一级关键词'], rrow['二级关键词'],
                                                                                   rrow['三级关键词']]
        p11 = '清洁能源|新能源'
        p12 = concat_rules([rrow['二级关键词'], rrow['三级关键词'], rrow['近义词']]) if len(rrow['四级关键词']) == 0 else concat_rules(
            [rrow['三级关键词'], rrow['四级关键词'], rrow['近义词']])
        d = rrow['删除词']
        r = rrow['排除词']
        basic_rule, recruit_rule = get_rules(p11,p12,d=d,r=r)
        newtags = row['三级关键词'] if len(rrow['四级关键词'])==0 else []
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 15_绿色化工产业
    sheet = '15_绿色化工产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词'], rrow['二级关键词']]
        p11 = '绿色化工|环保化工|有机化工'
        p12 = concat_rules([rrow['三级关键词'],rrow['二级关键词']])
        basic_rule, recruit_rule = get_rules(p11,p12)
        newtags = row['三级关键词']    
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")

    # 16_节能环保产业
    sheet = '16_节能环保产业'
    k = re.sub('(^.*_)|服务产业|产业|技术', '', sheet)
    ind_16p1 = ind_dict.get(k)
    for i in range(len(data[sheet])):
        row = data[sheet].iloc[i]
        rrow = row.apply(lambda x: concat_rules(list(
            map(lambda y: '[~a-zA-Z]' + y + '[~a-zA-Z]' if bool(re.search('[a-zA-Z]', y)) else '' if y == '其他' else y,
                x))) if type(x) == list else x)
        tag_list = [rrow['一级关键词'], rrow['二级关键词']]
        p11 = '节能|环保'
        p12 = concat_rules([rrow['三级关键词'], rrow['二级关键词']])
        newtags = row['三级关键词']    
        tmp = get_industry_company(ind_16p1,basic_rule,recruit_rule,tag_list,newtags)
        tmp.repartition(10).write.mode('append').csv(os.path.join(output_path, 'ind_16p1_companylist'), header=True)
    logger.info(f"====write {sheet} success====")


def save_to_data_share(entry, spark: SparkSession, input_path):
    """
    save data to data share path
    :param entry:
    :param spark:
    :param input_path:
    :return:
    """
    out_put_util = ResultOutputUtil(entry, 'company_tag_info', is_statistic=False, need_write_result=True)
    out_put_util.save(spark.read.csv(input_path, header=True).repartition(1000))


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    spark = entry.spark
    dw_name = entry.cfg_mgr.get('hive-dw', 'database')
    output_path = entry.cfg_mgr.hdfs.get_result_path('hdfs-biz', 'path_algorithm_tags')
    tmp_path = entry.cfg_mgr.hdfs.get_tmp_path('hdfs-biz', 'path_algorithm_tags')
    keywords_path = os.path.join(entry.cfg_mgr.get('biz', 'local_path'), 'biz_company_tags/keywords.xlsx')
    ind_dict = Industry.ind_16p1_desc_code_map()

    prepare_data(spark, dw_name, output_path)
    calc_ind_16p1(entry.logger, spark, ind_dict, output_path, keywords_path, tmp_path)
    save_to_data_share(entry, spark, os.path.join(output_path, 'ind_16p1_companylist'))


def post_check(entry: Entry):
    return True

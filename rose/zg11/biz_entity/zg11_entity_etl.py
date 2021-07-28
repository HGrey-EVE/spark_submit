#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: zg11上市公司画像实体
:Author: lirenchao@bbdservice.com
:Date: 22021-06-11 15:54
"""
import os
import re
import json
import time
import datetime
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import broadcast
from whetstone.core.entry import Entry
from zg11.proj_common.date_util import DateUtils
from zg11.proj_common.json_util import JsonUtils
from zg11.proj_common.hive_util import HiveUtil
from zg11.proj_common.mysql_util import MysqlUtils


def pre_check(entry: Entry):
    return True


def save_to_mysql(entry: Entry, df: DataFrame, table: str):
    """
    将数据保存到MySQL数据源，清空表再导入
    :param entry:
    :param df:
    :param table:
    :return:
    """
    host = entry.cfg_mgr.get('mysql', 'host')
    port = entry.cfg_mgr.get('mysql', 'port')
    dbname = entry.cfg_mgr.get('mysql', 'dbname')
    user = entry.cfg_mgr.get('mysql', 'user')
    passwd = entry.cfg_mgr.get('mysql', 'passwd')

    mysql_utils = MysqlUtils(host, port, dbname, user, passwd)
    mysql_utils.jdbc_write(df, table)


def save_to_hdfs(df: DataFrame, path: str):
    df.write.parquet(path, mode="overwrite")


def save_result(entry: Entry, df: DataFrame, args: dict):
    for key, value in args.items():
        if "MySQL" == key:
            pass
            # save_to_mysql(entry, df, value)
        elif "HDFS" == key:
            save_to_hdfs(df, value)


def to_json_object(line):
    """
    json字符串序列化
    """

    try:
        return json.loads(line, encoding='UTF-8')
    except Exception as ex:
        print(ex)
    return None


def map_for_group(x):
    """
    对stock_short,stock_code,company_name,report_date,rank进行分组
    :param x:
    :return:
    """
    return ((x['stock_short'], x['stock_code'], x['company_name'], x['report_date'], x['rank']), x)


def get_combining_trial_data(y):
    """
    迭代器转换,多个指标合为一行的处理
    """
    rows = list(y)
    r_dict = dict()

    r_dict['stock_short'] = rows[0]['stock_short']
    r_dict['stock_code'] = rows[0]['stock_code']
    r_dict['company_name'] = rows[0]['company_name']
    r_dict['report_date'] = rows[0]['report_date']
    r_dict['rank'] = rows[0]['rank']

    for i_index in range(len(rows)):
        for index_name in rows[i_index]:
            if index_name != 'stock_short' and index_name != 'stock_code' and index_name != 'company_name' \
                    and index_name != 'report_date' and index_name != 'rank':
                r_dict[index_name] = rows[i_index][index_name]
    return r_dict


def get_dataframe(entry: Entry, table_name) -> DataFrame:
    """
    输入表名，输出DataFrame
    :param entry:
    :param table_name:
    :return:
    """
    spark = entry.spark
    version = entry.version
    str2date = datetime.datetime.strptime(version, "%Y%m")
    current_month = DateUtils.add_date_2str(date_time=str2date, fmt="%Y%m", months=1)
    hive_database = entry.cfg_mgr.get('hive-dw', 'database')
    dt = HiveUtil.get_month_newest_partition(spark, table_name, current_month)
    df = spark.sql(
        f'''
        SELECT 
            *
        FROM {hive_database}.{table_name}
        WHERE dt={dt}
        '''
    )
    return df


# 百家姓
bjx = u'赵|钱|孙|李|周|吴|郑|王|冯|陈|褚|卫|蒋|沈|韩|杨|朱|秦|尤|许|何|吕|施|张|孔|曹|严|华|金|魏|陶|姜|戚|谢|邹|喻' \
      u'|柏|水|窦|章|云|苏|潘|葛|奚|范|彭|郎|鲁|韦|昌|马|苗|凤|花|方|俞|任|袁|柳|酆|鲍|史|唐|费|廉|岑|薛|雷|贺|倪' \
      u'|汤|滕|殷|罗|毕|郝|邬|安|常|乐|于|时|傅|皮|卞|齐|康|伍|余|元|卜|顾|孟|平|黄|和|穆|萧|尹|姚|邵|湛|汪|祁|毛' \
      u'|禹|狄|米|贝|明|臧|计|伏|成|戴|谈|宋|茅|庞|熊|纪|舒|屈|项|祝|董|粱|杜|阮|蓝|闵|席|季|麻|强|贾|路|娄|危|江' \
      u'|童|颜|郭|梅|盛|林|刁|钟|徐|邱|骆|高|夏|蔡|田|樊|胡|凌|霍|虞|万|支|柯|昝|管|卢|莫|经|房|裘|缪|干|解|应|宗' \
      u'|丁|宣|贲|邓|郁|单|杭|洪|包|诸|左|石|崔|吉|钮|龚|程|嵇|邢|滑|裴|陆|荣|翁|荀|羊|於|惠|甄|麴|家|封|芮|羿|储' \
      u'|靳|汲|邴|糜|松|井|段|富|巫|乌|焦|巴|弓|牧|隗|山|谷|车|侯|宓|蓬|全|郗|班|仰|秋|仲|伊|宫|宁|仇|栾|暴|甘|钭' \
      u'|厉|戎|祖|武|符|刘|景|詹|束|龙|叶|幸|司|韶|郜|黎|蓟|薄|印|宿|白|怀|蒲|邰|从|鄂|索|咸|籍|赖|卓|蔺|屠|蒙|池' \
      u'|乔|阴|欎|胥|能|苍|双|闻|莘|党|翟|谭|贡|劳|逄|姬|申|扶|堵|冉|宰|郦|雍|舄|璩|桑|桂|濮|牛|寿|通|边|扈|燕|冀' \
      u'|郏|浦|尚|农|温|别|庄|晏|柴|瞿|阎|充|慕|连|茹|习|宦|艾|鱼|容|向|古|易|慎|戈|廖|庾|终|暨|居|衡|步|都|耿|满' \
      u'|弘|匡|文|寇|广|禄|阙|东|殴|殳|沃|利|蔚|越|夔|隆|师|巩|厍|聂|晁|勾|敖|融|冷|訾|辛|阚|那|简|饶|空|曾|毋|沙' \
      u'|乜|养|鞠|须|丰|巢|关|蒯|相|查|後|荆|红|游|竺|权|逯|盖|益|桓|公|上|欧|赫|皇|尉|澹|淳|太|轩|令|宇|长|鲜|闾' \
      u'|亓|仉|督|子|颛|端|漆|壤|拓|夹|晋|楚|闫|法|汝|鄢|涂|钦|百|东|南|呼|归|海|微|岳|帅|缑|亢|况|后|有|琴|西|商' \
      u'|牟|佘|佴|伯|赏|墨|哈|谯|笪|年|爱|阳|佟|第|言|福|楼|勇|梁|鹿|付|仝|慧|肖|卿|苟|茆|瘳|覃|檀|片|代|青|珠|旷' \
      u'|服|衣|粟|基|纳|伞|热|尧|弥|次|操|扬|曲|门|敏|官|占|英|央|志|今|达|丘|刑|朴|扎|巨|尢|光|夕|佐|凃|风|延|色' \
      u'|延|由|敬|户|麦|芦|赛|兰|催|锁|占|雒|育|苓|隋|缐|来|但|才|宠|蹇|凡|宠|昂|知|丛|雪|候|蒿|原|阆|阿|忻|过|校' \
      u'|闪|伦|德|巾|缴|潭|将|礼|啕|星|钧|营|厚|艺|鮑|慈|郄|崖|嘉|维|傲|冶|买|革|丽|普|泰|卡|攀|北|疏|邸|协|洛|排' \
      u'|禤|同|展|矣|及|腾|苑|圣|折|静|露|随|迟|库|无|陕|庆|栗|闭|多|尕|畅|俄|栗|修|卲|宛|血|谌|布|句|练|战|立|化' \
      u'|智|紫|邝|依|佳|区|贠|竹|闰|开|冼|母|薜|承|是|侍|琼|辜|塔|知|本|位|丹|税|续|綦|揭|镡'.split('|')


def clean_customer_name(rows):
    rows = list(rows)
    re_list = list()
    for row in rows:
        customer_name = row.customer_name
        pattern_chinese = re.compile(r'[\u4e00-\u9fa5]+')
        pattern_english = re.compile(r'[a-zA-Z]+')
        if customer_name is not None:
            if len(re.findall(pattern_chinese, customer_name)) == 0 \
                    and len(re.findall(pattern_english, customer_name)) == 0:
                continue
        key_words = [u'供应商', u'供货商', u'医药商', u'实际控制人', u'同一实际控制人', u'军工', u'军品', u'排名第',
                     u'客户', u'单位', u'收入', u'销售商', u'经销商', u'自然人', u'同一控制', u'所属', u'下属']
        for word in key_words:
            if customer_name is None:
                break
            customer_name = customer_name.replace(word, u'')
        if customer_name is None:
            continue
        customer_name = re.split(u'（原|（含|（同|（购|（不|（总|（关联|（注|注', customer_name)[0]
        customer_name = re.split(u':|：', customer_name)[-1]
        customer_name_arr = re.split(
            u'及其下属分子公司|及其关联方|及其子公司|及其关联公司|及其下属子公司|及其附属公司|及子公司|及所属单位|'
            u'及其所属企业|及其实际控制的公司|及同一控制|及控股公司|及关联方|及下属企业|及所属企业|及其下属研究所|'
            u'下属子公司|及下属|及其主要关联企业|与|、|/', customer_name)
        for customer_name in customer_name_arr:
            if 0 < len(re.findall(r'第.*名', customer_name)) or 0 < len(re.findall(r'法人.*', customer_name)) \
                    or 0 < len(re.findall(r'单位.*', customer_name)) or 0 < len(re.findall(r'公司.*', customer_name)) \
                    or 0 < len(re.findall(r'第.*', customer_name)) or 0 < len(re.findall(r'第.*位', customer_name)) \
                    or 0 < len(re.findall(r'第.*持股会', customer_name)) or 0 < len(re.findall(r'丁公司', customer_name)) \
                    or 0 < len(re.findall(r'经销商.*', customer_name)) or 0 < len(re.findall(r'.*公司', customer_name))\
                    or 0 < len(re.findall(r'排名.*', customer_name)) or 0 < len(re.findall(r'海外.*', customer_name)) \
                    or 0 < len(re.findall(r'修理.*', customer_name)) or len(customer_name) < 2 \
                    or customer_name == 'null' or customer_name.isdigit():
                continue
            re_list.append(Row(stock_short=row.stock_short, stock_code=row.stock_code, company_name=row.company_name,
                               report_date=row.report_date, customer_name=customer_name, trade_amount=row.trade_amount,
                               trade_ratio=row.trade_ratio, rank=row.rank))
    return iter(re_list)


def clean_supplier_name(rows):
    rows = list(rows)
    re_list = list()
    for row in rows:
        supplier_name = row.supplier_name
        pattern_chinese = re.compile(r'[\u4e00-\u9fa5]+')
        pattern_english = re.compile(r'[a-zA-Z]+')
        if supplier_name is not None:
            if len(re.findall(pattern_chinese, supplier_name)) == 0 \
                    and len(re.findall(pattern_english, supplier_name)) == 0:
                continue
        key_words = [u'供应商', u'供货商', u'医药商', u'实际控制人', u'同一实际控制人', u'军工', u'军品', u'排名第',
                     u'客户', u'单位', u'收入', u'销售商', u'经销商', u'自然人', u'同一控制', u'所属', u'下属']
        for word in key_words:
            if supplier_name is None:
                break
            supplier_name = supplier_name.replace(word, u'')
        if supplier_name is None:
            continue
        supplier_name = re.split(u'（原|（含|（同|（购|（不|（总|（关联|（注|注', supplier_name)[0]
        supplier_name = re.split(u':|：', supplier_name)[-1]
        supplier_name_arr = re.split(
            u'及其下属分子公司|及其关联方|及其子公司|及其关联公司|及其下属子公司|及其附属公司|及子公司|及所属单位|'
            u'及其所属企业|及其实际控制的公司|及同一控制|及控股公司|及关联方|及下属企业|及所属企业|及其下属研究所|'
            u'下属子公司|及下属|及其主要关联企业|与|、|/', supplier_name)
        for supplier_name in supplier_name_arr:
            if 0 < len(re.findall(r'第.*名', supplier_name)) or 0 < len(re.findall(r'法人.*', supplier_name)) \
                    or 0 < len(re.findall(r'单位.*', supplier_name)) or 0 < len(re.findall(r'公司.*', supplier_name)) \
                    or 0 < len(re.findall(r'第.*', supplier_name)) or 0 < len(re.findall(r'第.*位', supplier_name)) \
                    or 0 < len(re.findall(r'第.*持股会', supplier_name)) or 0 < len(re.findall(r'丁公司', supplier_name)) \
                    or 0 < len(re.findall(r'经销商.*', supplier_name)) or 0 < len(re.findall(r'.*公司', supplier_name)) \
                    or 0 < len(re.findall(r'排名.*', supplier_name)) or 0 < len(re.findall(r'海外.*', supplier_name)) \
                    or 0 < len(re.findall(r'修理.*', supplier_name)) or len(supplier_name) < 2 \
                    or supplier_name == 'null' or supplier_name.isdigit():
                continue
            re_list.append(Row(stock_short=row.stock_short, stock_code=row.stock_code, company_name=row.company_name,
                               report_date=row.report_date, supplier_name=supplier_name, trade_amount=row.trade_amount,
                               trade_ratio=row.trade_ratio, rank=row.rank))
    return iter(re_list)


def column_parse(data, field, field_name, report_year, rank_value):
    """
    根据当前列，真实列名，报告日期，排名，进行列转列操作, eg:Row(stock_short, stock_code, company_name, field) =>
    Row(stock_short, stock_code, company_name, report_date, field_name, rank)
    :param data:
    :param field: 当前列
    :param field_name: 真实列名
    :param report_year: 报告日期
    :param rank_value: 排名
    :return:
    """
    # 将原始数据的英文括弧替换成中文括弧
    company_name = data['company_name'].replace('(', '（').replace(')', '）')
    row = Row(stock_short=data['stock_short'], stock_code=data['stock_code'], company_name=company_name,
              report_date=report_year, rank=rank_value)
    temp = row.asDict()
    field_value = data[field]
    # 将大客户和大供应商名称中英文括弧替换成中文括弧
    if (field_name == "大客户名称" or field_name == "大供应商名称") and data[field] != '' and data[field] is not None:
        field_value = data[field].replace('(', '（').replace(')', '）')
    temp[field_name] = field_value
    re_row = Row(**temp)
    return re_row


def company_name_clean(row):
    """
    上市公司名称清洗，将英文括弧转换成中文括弧
    :param row:
    :return:
    """
    company_name = row['company_name'].replace(')', '）').replace('(', '（')
    return Row(code=row.code, bbd_qyxx_id=row.bbd_qyxx_id, company_name=company_name, frname_compid=row.frname_compid,
               frname_id=row.frname_id, frname=row.frname, report_date=row.report_date, ulcontroller=row.ulcontroller)


def customer_compid_identify(rows):
    rows = list(rows)
    re_list = list()
    for row in rows:
        name = row.customer_name
        customer_compid = row.customer_compid
        if customer_compid == 0:
            re_list.append(Row(bbd_qyxx_id=row.bbd_qyxx_id, stock_short=row.stock_short, stock_code=row.stock_code,
                               company_name=row.company_name, report_date=row.report_date, customer_name=name,
                               customer_id=row.customer_id, customer_compid=0, trade_amount=row.trade_amount,
                               trade_ratio=row.trade_ratio, rank=row.rank))
        elif u'伊利' in name or u'西门子' in name or u'海尔系' in name or u'屈臣氏' in name or u'华为' in name \
             or u'艺龙网' in name or u'海信' in name or u'海尔' in name or u'富士康' in name or u'阿特斯' in name \
             or u'腾讯' in name or u'广达' in name or u'延锋系' in name or u'嘉盛' in name or u'百度' in name \
             or u'矽力杰' in name or u'欧比特' in name or u'沃特玛' in name or u'北奔' in name or u'陕汽' in name \
             or u'富士通' in name or u'紫光' in name or u'佳电' in name or u'和融勋' in name or u'泰克纳' in name \
             or u'时利和' in name or u'爱立信' in name or u'丁' in name or u'阿科玛' in name or u'阿克苏' in name \
             or u'阿里系' in name or u'阿斯莫' in name or u'安波福' in name or u'安克' in name or u'安踏' in name \
             or u'安信可' in name or u'安信特' in name or u'巴斯夫' in name or u'白莹' in name or u'白永祥' in name \
             or u'百得' in name or u'北高智' in name or u'贝尔金' in name or u'本特勒' in name or u'淳华' in name \
             or u'达明' in name or u'达能' in name or u'达瓦' in name or u'戴姆勒' in name or u'丹佛斯' in name \
             or u'德力西' in name or u'德润宝' in name or u'法雷奥' in name or u'丰禾原' in name or u'福特' in name \
             or u'福伊特' in name or u'光宇' in name or u'哈曼' in name or u'海福乐' in name or u'海佳系' in name \
             or u'海立' in name or u'海凌威' in name or u'海思科' in name or u'和而泰' in name or u'和利时' in name \
             or u'和硕' in name or u'赫斯基' in name or u'华峰' in name or u'华商龙' in name or u'华众' in name \
             or u'惠普' in name or u'吉利系' in name or u'家乐福' in name or u'嘉能可' in name or u'金佰利' in name \
             or u'金宝通' in name or u'金立' in name or u'金信诺' in name or u'景田' in name or u'卡拉罗' in name \
             or u'康美特' in name or u'康明斯' in name or u'柯尼卡' in name or u'兰蔻' in name or u'兰芝' in name \
             or u'乐达' in name or u'李尔' in name or u'李尔系' in name or u'利丰' in name or u'利郎' in name \
             or u'隆基系' in name or u'麦太保' in name or u'梅拉德' in name or u'敏实' in name or u'莫仕' in name \
             or u'牧田' in name or u'南昌' in name or u'欧菲光' in name or u'欧尚' in name or u'普莱德' in name \
             or u'普联' in name or u'齐普生' in name or u'强生' in name or u'荣之联' in name or u'赛科星' in name \
             or u'赛诺思' in name or u'施耐德' in name or u'时捷' in name or u'舒泰神' in name or u'松下系' in name \
             or u'苏泊尔' in name or u'索尼' in name or u'同景' in name or u'万宝至' in name or u'万科' in name \
             or u'王老吉' in name or u'沃尔玛' in name or u'武藏野' in name or u'协鑫系' in name or u'易力声' in name \
             or u'英特尔' in name or u'勇气' in name or u'元通系' in name or u'爱奇艺' in name or u'百力通' in name \
             or u'百隆' in name or u'本田' in name or u'德普乐' in name or u'德莎' in name or u'东顺翔' in name \
             or u'杜邦' in name or u'福禄克' in name or u'高通' in name or u'谷歌' in name or u'和升隆' in name \
             or u'基恩士' in name or u'金雅拓' in name or u'蓝格赛' in name or u'乐视' in name or u'路华' in name \
             or u'罗杰斯' in name or u'南高齿' in name or u'普立万' in name or u'普瑞得' in name or u'乔丰' in name \
             or u'是德' in name or u'双菱' in name or u'泰克' in name or u'易拉盖' in name or u'益弘泰' in name:
            re_list.append(Row(bbd_qyxx_id=row.bbd_qyxx_id, stock_short=row.stock_short, stock_code=row.stock_code,
                               company_name=row.company_name, report_date=row.report_date, customer_name=name,
                               customer_id=row.customer_id, customer_compid=2, trade_amount=row.trade_amount,
                               trade_ratio=row.trade_ratio, rank=row.rank))
        elif name[0] in bjx and 1 < len(name) < 4:
            re_list.append(Row(bbd_qyxx_id=row.bbd_qyxx_id, stock_short=row.stock_short, stock_code=row.stock_code,
                               company_name=row.company_name, report_date=row.report_date, customer_name=name,
                               customer_id=row.customer_id, customer_compid=1, trade_amount=row.trade_amount,
                               trade_ratio=row.trade_ratio, rank=row.rank))
        else:
            re_list.append(Row(bbd_qyxx_id=row.bbd_qyxx_id, stock_short=row.stock_short, stock_code=row.stock_code,
                               company_name=row.company_name, report_date=row.report_date, customer_name=name,
                               customer_id=row.customer_id, customer_compid=2, trade_amount=row.trade_amount,
                               trade_ratio=row.trade_ratio, rank=row.rank))
    return iter(re_list)


def supplier_compid_identify(rows):
    rows = list(rows)
    re_list = list()
    for row in rows:
        name = row.supplier_name
        supplier_compid = row.supplier_compid
        if supplier_compid == 0:
            re_list.append(Row(bbd_qyxx_id=row.bbd_qyxx_id, stock_short=row.stock_short, stock_code=row.stock_code,
                               company_name=row.company_name, report_date=row.report_date, supplier_name=name,
                               supplier_id=row.supplier_id, supplier_compid=0, trade_amount=row.trade_amount,
                               trade_ratio=row.trade_ratio, rank=row.rank))
        elif u'伊利' in name or u'西门子' in name or u'海尔系' in name or u'屈臣氏' in name or u'华为' in name \
                or u'艺龙网' in name or u'海信' in name or u'海尔' in name or u'富士康' in name or u'阿特斯' in name \
                or u'腾讯' in name or u'广达' in name or u'延锋系' in name or u'嘉盛' in name or u'百度' in name \
                or u'矽力杰' in name or u'欧比特' in name or u'沃特玛' in name or u'北奔' in name or u'陕汽' in name \
                or u'富士通' in name or u'紫光' in name or u'佳电' in name or u'和融勋' in name or u'泰克纳' in name \
                or u'时利和' in name or u'爱立信' in name or u'丁' in name or u'阿科玛' in name or u'阿克苏' in name \
                or u'阿里系' in name or u'阿斯莫' in name or u'安波福' in name or u'安克' in name or u'安踏' in name \
                or u'安信可' in name or u'安信特' in name or u'巴斯夫' in name or u'白莹' in name or u'白永祥' in name \
                or u'百得' in name or u'北高智' in name or u'贝尔金' in name or u'本特勒' in name or u'淳华' in name \
                or u'达明' in name or u'达能' in name or u'达瓦' in name or u'戴姆勒' in name or u'丹佛斯' in name \
                or u'德力西' in name or u'德润宝' in name or u'法雷奥' in name or u'丰禾原' in name or u'福特' in name \
                or u'福伊特' in name or u'光宇' in name or u'哈曼' in name or u'海福乐' in name or u'海佳系' in name \
                or u'海立' in name or u'海凌威' in name or u'海思科' in name or u'和而泰' in name or u'和利时' in name \
                or u'和硕' in name or u'赫斯基' in name or u'华峰' in name or u'华商龙' in name or u'华众' in name \
                or u'惠普' in name or u'吉利系' in name or u'家乐福' in name or u'嘉能可' in name or u'金佰利' in name \
                or u'金宝通' in name or u'金立' in name or u'金信诺' in name or u'景田' in name or u'卡拉罗' in name \
                or u'康美特' in name or u'康明斯' in name or u'柯尼卡' in name or u'兰蔻' in name or u'兰芝' in name \
                or u'乐达' in name or u'李尔' in name or u'李尔系' in name or u'利丰' in name or u'利郎' in name \
                or u'隆基系' in name or u'麦太保' in name or u'梅拉德' in name or u'敏实' in name or u'莫仕' in name \
                or u'牧田' in name or u'南昌' in name or u'欧菲光' in name or u'欧尚' in name or u'普莱德' in name \
                or u'普联' in name or u'齐普生' in name or u'强生' in name or u'荣之联' in name or u'赛科星' in name \
                or u'赛诺思' in name or u'施耐德' in name or u'时捷' in name or u'舒泰神' in name or u'松下系' in name \
                or u'苏泊尔' in name or u'索尼' in name or u'同景' in name or u'万宝至' in name or u'万科' in name \
                or u'王老吉' in name or u'沃尔玛' in name or u'武藏野' in name or u'协鑫系' in name or u'易力声' in name \
                or u'英特尔' in name or u'勇气' in name or u'元通系' in name or u'爱奇艺' in name or u'百力通' in name \
                or u'百隆' in name or u'本田' in name or u'德普乐' in name or u'德莎' in name or u'东顺翔' in name \
                or u'杜邦' in name or u'福禄克' in name or u'高通' in name or u'谷歌' in name or u'和升隆' in name \
                or u'基恩士' in name or u'金雅拓' in name or u'蓝格赛' in name or u'乐视' in name or u'路华' in name \
                or u'罗杰斯' in name or u'南高齿' in name or u'普立万' in name or u'普瑞得' in name or u'乔丰' in name \
                or u'是德' in name or u'双菱' in name or u'泰克' in name or u'易拉盖' in name or u'益弘泰' in name:
            re_list.append(Row(bbd_qyxx_id=row.bbd_qyxx_id, stock_short=row.stock_short, stock_code=row.stock_code,
                               company_name=row.company_name, report_date=row.report_date, supplier_name=name,
                               supplier_id=row.supplier_id, supplier_compid=2, trade_amount=row.trade_amount,
                               trade_ratio=row.trade_ratio, rank=row.rank))
        elif name[0] in bjx and 1 < len(name) < 4:
            re_list.append(Row(bbd_qyxx_id=row.bbd_qyxx_id, stock_short=row.stock_short, stock_code=row.stock_code,
                               company_name=row.company_name, report_date=row.report_date, supplier_name=name,
                               supplier_id=row.supplier_id, supplier_compid=1, trade_amount=row.trade_amount,
                               trade_ratio=row.trade_ratio, rank=row.rank))
        else:
            re_list.append(Row(bbd_qyxx_id=row.bbd_qyxx_id, stock_short=row.stock_short, stock_code=row.stock_code,
                               company_name=row.company_name, report_date=row.report_date, supplier_name=name,
                               supplier_id=row.supplier_id, supplier_compid=2, trade_amount=row.trade_amount,
                               trade_ratio=row.trade_ratio, rank=row.rank))
    return iter(re_list)


def data_path(entry):
    """
    统一管理数据存储路径
    :param entry:
    :return:
    """
    # 外部数据路径
    input_path = entry.cfg_mgr.hdfs.get_fixed_path("hdfs", "hdfs_input_path")
    # 实体模块临时数据路径
    entity_tmp_path = entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path")
    # basic模块结果数据路径，实体模块需依赖基本信息模块数据
    basic_result_path = entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_basic_path")
    # 基本信息模块临时数据路径
    basic_tmp_path = entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_basic_path")

    # 公司类型映射表数据路径
    company_type_path = os.path.join(input_path, "company_type.csv")
    # 上市公司关联方表数据路径
    listed_relation_company_path = os.path.join(basic_result_path, "listed_relation_company", "*")
    # 上市公司表数据路径
    listed_name_path = os.path.join(basic_result_path, "listed_name", "*")
    # 企业基本信息表数据路径
    qyxx_basic_path = os.path.join(basic_tmp_path, "qyxx_basic")
    # 省份映射表数据路径
    province_path = os.path.join(input_path, "province.csv")
    # 城市映射表数据路径
    city_path = os.path.join(input_path, "city.csv")
    # 上市公司唯一法人表数据路径
    listed_person_unique_path = os.path.join(input_path, "listed_person_unique.csv")
    # 上市公司大客户和大供应商文件路径
    listed_company_customer_supplier_path = os.path.join(input_path, "listed_company_customer_supplier.csv")
    # 大客户和大供应商临时数据中转目录
    transit_path = os.path.join(entity_tmp_path, "customer_supplier")
    # 上市公司大客户数据临时路径
    qyxx_ipo_customer_path = os.path.join(entity_tmp_path, "qyxx_ipo_customer")
    # 上市公司大供应商数据临时路径
    qyxx_ipo_supplier_path = os.path.join(entity_tmp_path, "qyxx_ipo_supplier")
    # 存储合并后解析列
    listed_company_customer_supplier_tmp_path = os.path.join(transit_path, "all")
    # 分子公司结果存储路径
    company_branch_save_path = os.path.join(entity_tmp_path, 'vertex_company_branch')
    # 公司数据保存路径
    company_save_path = os.path.join(entity_tmp_path, 'vertex_company')
    # 公司高管或投资人数据保存路径
    person_save_path = os.path.join(entity_tmp_path, 'vertex_person')
    # 地址数据存储路径
    address_save_path = os.path.join(entity_tmp_path, 'vertex_address')
    # 联系电话数据存储路径
    phonenumber_save_path = os.path.join(entity_tmp_path, 'vertex_phonenumber')
    # 电子邮箱数据存储路径
    email_save_path = os.path.join(entity_tmp_path, 'vertex_email')
    # 组织机构代码数据存储路径
    orgcode_save_path = os.path.join(entity_tmp_path, 'vertex_orgcode')
    # 处罚数据存储路径
    punish_save_path = os.path.join(entity_tmp_path, 'vertex_punish')
    # 司法数据存储路径
    judicial_save_path = os.path.join(entity_tmp_path, 'vertex_judicial')
    # 失信名单数据存储路径
    bfat_save_path = os.path.join(entity_tmp_path, 'vertex_bfat')

    path_dict = {'company_type_path': company_type_path,
                 'listed_relation_company_path': listed_relation_company_path,
                 'listed_name_path': listed_name_path,
                 'qyxx_basic_path': qyxx_basic_path,
                 'province_path': province_path,
                 'city_path': city_path,
                 'listed_person_unique_path': listed_person_unique_path,
                 'listed_company_customer_supplier_path': listed_company_customer_supplier_path,
                 'transit_path': transit_path,
                 'qyxx_ipo_customer_path': qyxx_ipo_customer_path,
                 'qyxx_ipo_supplier_path': qyxx_ipo_supplier_path,
                 'listed_company_customer_supplier_tmp_path': listed_company_customer_supplier_tmp_path,
                 'company_branch_save_path': company_branch_save_path,
                 'company_save_path': company_save_path,
                 'person_save_path': person_save_path,
                 'address_save_path': address_save_path,
                 'phonenumber_save_path': phonenumber_save_path,
                 'email_save_path': email_save_path,
                 'orgcode_save_path': orgcode_save_path,
                 'punish_save_path': punish_save_path,
                 'judicial_save_path': judicial_save_path,
                 'bfat_save_path': bfat_save_path}
    return path_dict


def extract_basic_data(entry: Entry, path_dict: dict) -> dict:
    spark = entry.spark
    logger = entry.logger
    logger.info("entity_module start checking basic data")

    try:
        # 读取公司类型表数据
        spark.read.csv(path_dict['company_type_path'], header=True, sep='\t').createOrReplaceTempView("company_type")

        # 读取上市公司关联方表数据
        listed_relation_company = spark.read.json(path_dict['listed_relation_company_path'])\
            .where("bbd_qyxx_id != '' and bbd_qyxx_id is not null")\
            .select("bbd_qyxx_id")\
            .distinct()\
            .repartition(2)\
            .persist(StorageLevel.MEMORY_AND_DISK)
        listed_relation_company.createOrReplaceTempView("listed_relation_company")

        # 读取上市公司表数据
        listed_name = spark.read.json(path_dict['listed_name_path']).rdd\
            .map(company_name_clean).toDF()\
            .select("bbd_qyxx_id", "company_name", "report_date", "frname_id", "frname", "frname_compid", "code",
                    "ulcontroller")\
            .persist(StorageLevel.MEMORY_AND_DISK)
        listed_name.createOrReplaceTempView("listed_name")

        # 筛选上市公司企业信息编号
        listed_name_qyxx_id = listed_name.select("bbd_qyxx_id").distinct().persist(StorageLevel.MEMORY_AND_DISK)
        listed_name_qyxx_id.createOrReplaceTempView("listed_name_qyxx_id")

        # 读取省份映射表数据
        spark.read.csv(path_dict['province_path'], header=True, sep='\t').createOrReplaceTempView("province")

        # 读取城市映射表数据
        spark.read.csv(path_dict['city_path'], header=True, sep='\t').createOrReplaceTempView("city")

        # 读取上市公司唯一法人表数据
        spark.read.csv(path_dict['listed_person_unique_path'], header=True)\
            .select("bbd_qyxx_id", "uuid", "name")\
            .repartition(10)\
            .createOrReplaceTempView("listed_person_unique")

        # 读取上市公司大客户和大供应商数据
        listed_company_customer_supplier_path = path_dict['listed_company_customer_supplier_path']
        listed_company_customer_supplier = spark.read.csv(listed_company_customer_supplier_path, header=True)

        # 获取qyxx_basic数据
        qyxx_basic = spark.read.parquet(path_dict['qyxx_basic_path'])\
            .select("bbd_qyxx_id", "company_name", "company_type", "credit_code", "frname", "frname_id",
                    "company_industry", "form", "openfrom", "opento", "operate_scope", "regcap_amount",
                    "regcap_currency", "history_name", "company_enterprise_status", "esdate", "cancel_date", "address")\
            .repartition(30)\
            .persist(StorageLevel.MEMORY_AND_DISK)
        qyxx_basic.createOrReplaceTempView("qyxx_basic")

        # 获取basis_alias_library指定分区数据
        get_dataframe(entry, 'basis_alias_library')\
            .where("byname_type rlike '历史名称|企业简称|英文名称|股票简称'")\
            .select("bbd_qyxx_id", "byname")\
            .repartition(20)\
            .createOrReplaceTempView("basis_alias_library")

        # 获取qyxx_baxx指定分区数据
        get_dataframe(entry, 'qyxx_baxx')\
            .select("name_id", "name", "position", "bbd_qyxx_id")\
            .join(listed_relation_company, ["bbd_qyxx_id"]).rdd\
            .map(lambda row: (row.bbd_qyxx_id, row))\
            .repartition(20) \
            .map(lambda row: row[1]) \
            .toDF()\
            .createOrReplaceTempView("qyxx_baxx")

        # 获取qyxx_gdxx指定分区数据
        get_dataframe(entry, 'qyxx_gdxx') \
            .where("shareholder_name != '' and shareholder_name is not null "
                   "and company_name != '' and company_name is not null")\
            .select("bbd_qyxx_id", "company_name", "shareholder_id", "shareholder_name", "invest_ratio",
                    "invest_amount", "paid_detail")\
            .join(listed_relation_company, ["bbd_qyxx_id"]).rdd\
            .map(lambda row: (row.bbd_qyxx_id, row))\
            .repartition(30)\
            .map(lambda row: row[1])\
            .toDF().createOrReplaceTempView("qyxx_gdxx")

        # 获取legal_dishonest_persons_subject_to_enforcement指定分区数据
        get_dataframe(entry, 'legal_dishonest_persons_subject_to_enforcement')\
            .select("bbd_xgxx_id", "dishonest_situation", "register_date", "exec_state", "bbd_qyxx_id",
                    "exec_obligation", "case_code")\
            .join(broadcast(listed_name_qyxx_id), ["bbd_qyxx_id"])\
            .repartition(1)\
            .createOrReplaceTempView("legal_dishonest_persons_subject_to_enforcement")

        # 获取xzcf指定分区数据
        get_dataframe(entry, 'xzcf')\
            .select("bbd_xgxx_id", "public_date", "punish_category_one", "punish_content", "bbd_qyxx_id",
                    "punish_code")\
            .join(listed_relation_company, ["bbd_qyxx_id"])\
            .repartition(1)\
            .createOrReplaceTempView("xzcf")

        # 获取qyxx_xzcf指定分区数据
        get_dataframe(entry, 'qyxx_xzcf')\
            .select("bbd_unique_id", "punish_date", "punish_type", "punish_content", "bbd_qyxx_id", "punish_code")\
            .join(listed_relation_company, ["bbd_qyxx_id"])\
            .repartition(1)\
            .createOrReplaceTempView("qyxx_xzcf")

        # 获取qyxg_circxzcf指定分区数据
        get_dataframe(entry, 'qyxg_circxzcf')\
            .select("bbd_xgxx_id", "pubdate", "type", "main", "bbd_qyxx_id")\
            .join(listed_relation_company, ["bbd_qyxx_id"])\
            .repartition(1)\
            .createOrReplaceTempView("qyxg_circxzcf")

        # 获取legal_court_notice指定分区数据
        get_dataframe(entry, 'legal_court_notice')\
            .select("bbd_xgxx_id", "notice_type", "notice_time", "bbd_qyxx_id", "notice_content", "accuser",
                    "defendant", "case_type")\
            .join(broadcast(listed_name_qyxx_id), ["bbd_qyxx_id"])\
            .rdd.map(lambda row: (row.bbd_xgxx_id, row))\
            .repartition(4)\
            .map(lambda row: row[1])\
            .toDF()\
            .createOrReplaceTempView("legal_court_notice")

        # 获取ktgg指定分区数据
        get_dataframe(entry, 'ktgg')\
            .select("bbd_xgxx_id", "action_cause", "trial_date", "bbd_qyxx_id", "main", "accuser", "defendant",
                    "case_type")\
            .join(broadcast(listed_name_qyxx_id), ["bbd_qyxx_id"])\
            .repartition(1)\
            .createOrReplaceTempView("ktgg")

        # 获取legal_adjudicative_documents指定分区数据
        get_dataframe(entry, 'legal_adjudicative_documents')\
            .where("bbd_qyxx_id is not null")\
            .select("bbd_xgxx_id", "action_cause", "sentence_date", "bbd_qyxx_id", "notice_content",
                    "litigation_status", "case_type")\
            .join(broadcast(listed_name_qyxx_id), ["bbd_qyxx_id"]).rdd\
            .map(lambda row: (row.bbd_xgxx_id, row))\
            .repartition(400)\
            .map(lambda row: row[1])\
            .toDF().createOrReplaceTempView("legal_adjudicative_documents")

        # 获取legal_persons_subject_to_enforcement指定分区数据
        get_dataframe(entry, 'legal_persons_subject_to_enforcement')\
            .select("bbd_xgxx_id", "register_date", "bbd_qyxx_id", "exec_court_name", "case_code", "pname")\
            .join(broadcast(listed_name_qyxx_id), ["bbd_qyxx_id"])\
            .repartition(1)\
            .createOrReplaceTempView("legal_persons_subject_to_enforcement")

        # 获取qyxx_annual_report_jbxx指定分区数据
        get_dataframe(entry, 'qyxx_annual_report_jbxx')\
            .select("bbd_qyxx_id", "phone", "email", "year")\
            .join(listed_relation_company, ["bbd_qyxx_id"]).rdd\
            .map(lambda row: (row.bbd_qyxx_id, row))\
            .repartition(15)\
            .map(lambda row: row[1])\
            .toDF()\
            .createOrReplaceTempView("qyxx_annual_report_jbxx")
    except Exception as e:
        import traceback
        logger.error("#########################################Exception##########################################")
        logger.error("Failed to check the basic data!")
        traceback.print_exc()
        raise e
    # 行业映射视图
    industry_map = [{'key': 'A', 'value': u'农、林、牧、渔业'},
                    {'key': 'B', 'value': u'采矿业'},
                    {'key': 'C', 'value': u'制造业'},
                    {'key': 'D', 'value': u'电力、热力、燃气及水生产和供应业'},
                    {'key': 'E', 'value': u'建筑业'},
                    {'key': 'F', 'value': u'批发和零售业'},
                    {'key': 'G', 'value': u'交通运输、仓储和邮政业'},
                    {'key': 'H', 'value': u'住宿和餐饮业'},
                    {'key': 'I', 'value': u'信息传输、软件和信息技术服务业'},
                    {'key': 'J', 'value': u'金融业'},
                    {'key': 'K', 'value': u'房地产业'},
                    {'key': 'L', 'value': u'租赁和商务服务业'},
                    {'key': 'M', 'value': u'科学研究和技术服务业'},
                    {'key': 'N', 'value': u'水利、环境和公共设施管理业'},
                    {'key': 'O', 'value': u'居民服务、修理和其他服务业'},
                    {'key': 'P', 'value': u'教育'},
                    {'key': 'Q', 'value': u'卫生和社会工作'},
                    {'key': 'R', 'value': u'文化、体育和娱乐业'},
                    {'key': 'S', 'value': u'综合'}]
    spark.createDataFrame(industry_map).createOrReplaceTempView('industry_map')
    logger.info("entity_module has completed basic data check")
    df_dict = {'listed_relation_company': listed_relation_company,
               'listed_name': listed_name,
               'qyxx_basic': qyxx_basic,
               'listed_name_qyxx_id': listed_name_qyxx_id,
               'listed_company_customer_supplier': listed_company_customer_supplier}
    return df_dict


def qyxx_ipo_customer_and_supplier(entry: Entry, df_dict: dict, path_dict: dict):
    """
    解析上市公司大客户和大供应商原始数据，生成结构化上市公司大供应商和大客户数据
    :param entry:
    :param df_dict:
    :param path_dict:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    logger.info("entity_module start generating qyxx_ipo_customer and qyxx_ipo_supplier table")
    # 大客户和大供应商临时数据中转目录
    transit_path = path_dict['transit_path']
    # 上市公司大客户数据临时路径
    qyxx_ipo_customer_path = path_dict['qyxx_ipo_customer_path']
    # 上市公司大供应商数据临时路径
    qyxx_ipo_supplier_path = path_dict['qyxx_ipo_supplier_path']
    # 存储合并后解析列
    listed_company_customer_supplier_tmp_path = path_dict['listed_company_customer_supplier_tmp_path']

    # 上市公司大客户和大供应商数据
    listed_company_customer_supplier = df_dict['listed_company_customer_supplier']
    # 获取listed_company_customer_supplier所有列
    columns = listed_company_customer_supplier.columns
    # 容器，存储股票代码, 股票简称, 公司名称外的所有列
    fields = []
    # 遍历listed_company_customer_supplier所有列，将股票代码, 股票简称, 公司名称外的所有列追加到fields
    for column in columns:
        if column not in [u'股票代码', u'股票简称', u'公司名称']:
            fields.append(column)
    index = 0
    os.system("hdfs dfs -rm -r -f " + transit_path)
    for field in fields:
        # field：大客户销售收入占比[报告期]2016年报[排名]第1名[单位]%
        splits = field.split("[")
        # 提取该列真实列名
        field_name = splits[0]
        report_date_field_value = splits[1].split("]")
        # 提取报告年份
        report_year = re.findall(r"\d+", report_date_field_value[1])[0]
        # 提取报告类别
        report_type = re.findall("[\u4e00-\u9fa5]+", report_date_field_value[1])[0]
        # 提取排名
        rank_value = re.findall(r"\d+", splits[2].split("]")[1])[0]
        # 组装报告日期
        if report_type == u'年报':
            report_year = report_year + '1231'
        # 进行列转列
        listed_company_customer_supplier.selectExpr(
            "`股票代码` as stock_code",
            "`股票简称` as stock_short",
            "`公司名称` as company_name",
            f"`{field}`") \
            .rdd \
            .map(lambda row: column_parse(row, field, field_name, report_year, rank_value)) \
            .map(lambda row: JsonUtils.to_string(row.asDict())) \
            .saveAsTextFile(os.path.join(transit_path, str(index)))
        index = index + 1

    os.system("hdfs dfs -rm -r -f " + listed_company_customer_supplier_tmp_path)

    # 合并解析列
    spark.sparkContext.textFile(os.path.join(transit_path, "*", "*")) \
        .map(to_json_object) \
        .map(lambda x: map_for_group(x)) \
        .groupByKey() \
        .map(lambda row: get_combining_trial_data(row[1])) \
        .map(lambda row: JsonUtils.to_string(row)) \
        .saveAsTextFile(listed_company_customer_supplier_tmp_path)
    logger.info("listed company customer and supplier data has completed analyze")
    # 读取合并后listed_company_customer_supplier数据
    spark.read.json(os.path.join(listed_company_customer_supplier_tmp_path, "*")).distinct() \
        .createOrReplaceTempView("listed_company_customer_supplier")
    logger.info("entity_module start generating qyxx_ipo_customer table")
    # 上市公司大客户数据
    spark.sql(
        '''
        SELECT 
            stock_short,
            stock_code,
            company_name,
            report_date,
            `大客户名称` AS customer_name,
            regexp_replace(`大客户销售收入`, ',', '') AS trade_amount,
            regexp_replace(`大客户销售收入占比`, ',', '') AS trade_ratio,
            rank
        FROM listed_company_customer_supplier
        WHERE `大客户名称` IS NOT NULL
        AND `大客户名称` NOT RLIKE '[a-z][股份,有限]'
        '''
    ).rdd.mapPartitions(clean_customer_name).toDF().distinct().createOrReplaceTempView("qyxx_ipo_customer")

    qyxx_ipo_customer = spark.sql(
        '''
        SELECT
            b.bbd_qyxx_id,
            a.stock_short,
            a.stock_code,
            a.company_name,
            a.report_date,
            a.customer_name,
            CASE WHEN c.bbd_qyxx_id IS NOT NULL THEN c.bbd_qyxx_id 
                    WHEN c.bbd_qyxx_id IS NULL AND d.bbd_qyxx_id IS NOT NULL THEN d.bbd_qyxx_id 
                    WHEN c.bbd_qyxx_id IS NULL AND d.bbd_qyxx_id IS NULL AND LENGTH(a.customer_name) > 4 
                    THEN MD5(a.customer_name) ELSE '' END AS customer_id,
            CASE WHEN c.bbd_qyxx_id IS NOT NULL OR d.bbd_qyxx_id IS NOT NULL THEN 0 ELSE 2 END AS customer_compid,
            a.trade_amount,
            a.trade_ratio,
            a.rank
        FROM qyxx_ipo_customer a
        JOIN qyxx_basic b
        ON a.company_name=b.company_name
        LEFT JOIN qyxx_basic c
        ON a.customer_name=c.company_name
        LEFT JOIN basis_alias_library d
        ON a.customer_name=d.byname
        '''
    ).rdd.mapPartitions(customer_compid_identify).toDF().distinct().repartition(1)
    qyxx_ipo_customer.write.csv(qyxx_ipo_customer_path, header=True, mode="overwrite")
    qyxx_ipo_customer.createOrReplaceTempView("qyxx_ipo_customer")
    logger.info(f"entity_module has generated qyxx_ipo_customer table and written to the path {qyxx_ipo_customer_path}")

    logger.info("entity_module start generating qyxx_ipo_supplier table")
    # 上市公司供应商数据
    spark.sql(
        '''
        SELECT 
            stock_short,
            stock_code,
            company_name,
            report_date,
            `大供应商名称` AS supplier_name,
            regexp_replace(`大供应商采购金额`, ',', '') AS trade_amount,
            regexp_replace(`大供应商采购金额占比`, ',', '') AS trade_ratio,
            rank
        FROM listed_company_customer_supplier
        WHERE `大供应商名称` IS NOT NULL
        AND `大供应商名称` NOT RLIKE '[a-z][股份,有限]'
        '''
    ).rdd.mapPartitions(clean_supplier_name).toDF().createOrReplaceTempView("qyxx_ipo_supplier")

    qyxx_ipo_supplier = spark.sql(
        '''
        SELECT
            b.bbd_qyxx_id,
            a.stock_short,
            a.stock_code,
            a.company_name,
            a.report_date,
            a.supplier_name,
            CASE WHEN c.bbd_qyxx_id IS NOT NULL THEN c.bbd_qyxx_id 
                    WHEN c.bbd_qyxx_id IS NULL AND d.bbd_qyxx_id IS NOT NULL THEN d.bbd_qyxx_id 
                    WHEN c.bbd_qyxx_id IS NULL AND d.bbd_qyxx_id IS NULL AND LENGTH(a.supplier_name) > 4 
                    THEN MD5(a.supplier_name) ELSE '' END AS supplier_id,
            CASE WHEN c.bbd_qyxx_id IS NOT NULL OR d.bbd_qyxx_id IS NOT NULL THEN 0 ELSE 2 END AS supplier_compid,
            a.trade_amount,
            a.trade_ratio,
            a.rank
        FROM qyxx_ipo_supplier a
        JOIN qyxx_basic b
        ON a.company_name=b.company_name
        LEFT JOIN qyxx_basic c
        ON a.supplier_name=c.company_name
        LEFT JOIN basis_alias_library d
        ON a.supplier_name=d.byname
        '''
    ).rdd.mapPartitions(supplier_compid_identify).toDF().distinct().repartition(1)

    qyxx_ipo_supplier.write.csv(qyxx_ipo_supplier_path, header=True, mode="overwrite")
    qyxx_ipo_supplier.createOrReplaceTempView("qyxx_ipo_supplier")
    logger.info(f"entity_module has generated qyxx_ipo_supplier table and written to the path {qyxx_ipo_supplier_path}")
    # 删除中转目录临时数据
    code = os.system("hdfs dfs -rm -r -f " + transit_path)
    if code != 0:
        logger.error(f"failed to delete listed company customer and supplier temp data, error code is {code}")
    else:
        logger.info("listed company customer and supplier temp data has deleted")
    logger.info("entity_module has generated qyxx_ipo_customer and qyxx_ipo_supplier table")


def vertex_company_branch(entry: Entry, path_dict: dict):
    """
    分子公司
    :param entry:
    :param path_dict:
    :return:
    """
    spark = entry.spark
    logger = entry.logger

    logger.info("entity_module start generating vertex_company_branch table")
    company_branch = spark.sql(
        '''
        SELECT 
            b.bbd_qyxx_id AS uid,
            b.company_name AS name
        FROM listed_name a
        JOIN(
                SELECT 
                    l.bbd_qyxx_id,
                    l.company_name,
                    l.shareholder_id
                FROM qyxx_gdxx l
                LEFT JOIN listed_name r
                ON l.bbd_qyxx_id=r.bbd_qyxx_id
                WHERE r.bbd_qyxx_id IS NULL
            ) b
        ON a.bbd_qyxx_id=b.shareholder_id
        UNION 
        SELECT 
            a.bbd_qyxx_id AS uid,
            a.company_name AS name
        FROM qyxx_basic a
        JOIN listed_relation_company b
        ON a.company_name RLIKE '分公司|分厂|分店|分支机构|分行|支行|营业部' AND a.bbd_qyxx_id=b.bbd_qyxx_id
        '''
    ).repartition(1)

    # 数据入MySQL和写HDFS
    args = {'MySQL': 'company_branch', 'HDFS': path_dict['company_branch_save_path']}
    save_result(entry, company_branch, args)

    company_branch.createOrReplaceTempView("vertex_company_branch")
    logger.info(f"vertex_company_branch table has been written to hdfs path {path_dict['company_branch_save_path']}")
    logger.info("entity_module has generated vertex_company_branch table")


def vertex_company(entry: Entry, path_dict: dict):
    """
    公司实体
    :param entry:
    :param path_dict:
    :return:
    """
    spark = entry.spark
    logger = entry.logger

    logger.info("entity_module start generating vertex_company table")
    part1 = spark.sql(
        '''
        SELECT 
            a.bbd_qyxx_id AS uid,
            a.company_name AS name,
            a.company_type AS type,
            a.company_industry,
            a.form,
            a.openfrom,
            a.opento,
            a.operate_scope AS range,
            a.regcap_amount AS regcapamount,
            a.regcap_currency AS regcapcurrency,
            '' AS relatedid,
            '' AS nameid,
            '' AS islowcap,
            CASE WHEN b.name IS NOT NULL THEN '子/分公司' ELSE '' END AS branch,
            a.history_name,
            '' AS relatedtype,
            '' AS trade_amount,
            '' AS isnew,
            '' AS iscancel,
            a.company_enterprise_status AS enterprise_status
        FROM qyxx_basic a 
        LEFT JOIN vertex_company_branch b 
        ON a.company_name=b.name
        '''
    )

    spark.sql(
        '''
         SELECT
            *
         FROM qyxx_ipo_customer
         WHERE customer_id IS NOT NULL
        '''
    ).createOrReplaceTempView("qyxx_ipo_customer_tmp")
    part2 = spark.sql(
        '''
        SELECT 
            a.customer_id AS uid,
            a.customer_name AS name,
            b.company_type AS type,
            b.company_industry,
            b.form,
            b.openfrom,
            b.opento,
            b.operate_scope AS range,
            b.regcap_amount AS regcapamount,
            b.regcap_currency AS regcapcurrency,
            a.bbd_qyxx_id AS relatedid,
            a.report_date AS nameid,
            CASE WHEN (b.regcap_amount IS NOT NULL AND b.regcap_amount != 0 AND b.regcap_currency == '美元' 
                    AND a.trade_amount / b.regcap_amount * 6.7 > 10) THEN '是' 
                    WHEN (b.regcap_amount IS NOT NULL AND b.regcap_amount != 0 
                    AND a.trade_amount / b.regcap_amount > 10) THEN '是' 
                    ELSE '否' END AS islowcap,
            CASE WHEN c.uid IS NOT NULL THEN '子/分公司' ELSE '' END AS branch,
            b.history_name,
            '客户' AS relatedtype,
            a.trade_amount,
            CASE WHEN b.esdate IS NOT NULL AND DATE_FORMAT(ADD_MONTHS(b.esdate, 12), 'yyyy') >= a.report_date THEN '是' 
                    ELSE '否' END AS isnew,
            CASE WHEN b.cancel_date IS NOT NULL AND DATE_FORMAT(ADD_MONTHS(b.cancel_date, -12), 'yyyy') <= a.report_date 
                    THEN '是' ELSE '否' END AS iscancel,
            b.company_enterprise_status AS enterprise_status
        FROM qyxx_ipo_customer_tmp a
        LEFT JOIN qyxx_basic b
        ON a.customer_id=b.bbd_qyxx_id
        LEFT JOIN vertex_company_branch c
        ON a.customer_id=c.uid
        '''
    )

    spark.sql(
        '''
        SELECT 
            *
        FROM qyxx_ipo_supplier
        WHERE supplier_id IS NOT NULL
        '''
    ).createOrReplaceTempView("qyxx_ipo_supplier_tmp")
    part3 = spark.sql(
        '''
        SELECT 
            a.supplier_id AS uid,
            a.supplier_name AS name,
            b.company_type AS type,
            b.company_industry,
            b.form,
            b.openfrom,
            b.opento,
            b.operate_scope AS range,
            b.regcap_amount AS regcapamount,
            b.regcap_currency AS regcapcurrency,
            a.bbd_qyxx_id AS relatedid,
            a.report_date AS nameid,
            CASE WHEN (b.regcap_amount IS NOT NULL AND b.regcap_amount != 0 AND b.regcap_currency == '美元' 
                    AND a.trade_amount / b.regcap_amount * 6.7 > 10) THEN '是' 
                    WHEN (b.regcap_amount IS NOT NULL AND b.regcap_amount != 0 
                    AND a.trade_amount / b.regcap_amount > 10) THEN '是'
                    ELSE '否' END AS islowcap,
            CASE WHEN c.uid IS NOT NULL THEN '子/分公司' ELSE '' END AS branch,
            b.history_name,
            '供应商' AS relatedtype,
            a.trade_amount,
            CASE WHEN b.esdate IS NOT NULL AND DATE_FORMAT(ADD_MONTHS(b.esdate, 12), 'yyyy') >= a.report_date THEN '是' 
                    ELSE '否' END AS isnew,
            CASE WHEN b.cancel_date IS NOT NULL AND DATE_FORMAT(ADD_MONTHS(b.cancel_date, -12), 'yyyy') <= a.report_date 
                    THEN '是' ELSE '否' END AS iscancel,
            b.company_enterprise_status AS enterprise_status
        FROM qyxx_ipo_supplier_tmp a
        LEFT JOIN qyxx_basic b
        ON a.supplier_id=b.bbd_qyxx_id
        LEFT JOIN vertex_company_branch c
        ON a.supplier_id=c.uid
        '''
    )

    def name_split(rows):
        rows = list(rows)
        re_list = list()
        for row in rows:
            ulcontroller = row.ulcontroller
            ulcontrollers = ulcontroller.split('（')[0].split('、')
            for name in ulcontrollers:
                if name == 'null' or name.isdigit():
                    continue
                re_list.append(Row(name=name, report_date=row.report_date))
        return iter(re_list)

    spark.sql(
        '''
        SELECT 
            ulcontroller,
            report_date
        FROM listed_name
        WHERE ulcontroller IS NOT NULL
        '''
    ).rdd.mapPartitions(name_split).toDF()\
        .where("name != ''")\
        .createOrReplaceTempView("part4_tmp")

    part4 = spark.sql(
        '''
        SELECT 
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.bbd_qyxx_id
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.bbd_qyxx_id
                    ELSE MD5(a.name) END AS uid,
            a.name AS name,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.company_type
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.company_type
                    ELSE '' END AS type,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.company_industry
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.company_industry
                    ELSE '' END AS company_industry,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.form
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.form
                    ELSE '' END AS form,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.openfrom
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.openfrom
                    ELSE '' END AS openfrom,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.opento
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.opento
                    ELSE '' END AS opento,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.operate_scope
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.operate_scope
                    ELSE '' END AS range,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.regcap_amount
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.regcap_amount
                    ELSE '' END AS regcapamount,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.regcap_currency
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.regcap_currency
                    ELSE '' END AS regcapcurrency,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.bbd_qyxx_id
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.bbd_qyxx_id
                    ELSE '' END AS relatedid,
            a.report_date AS nameid,
            '' AS islowcap,
            CASE WHEN (b.bbd_qyxx_id IS NOT NULL OR c.bbd_qyxx_id IS NOT NULL) AND d.uid IS NOT NULL THEN '子/分公司' 
                    ELSE '' END AS branch,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.history_name
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.history_name
                    ELSE '' END AS history_name,
            '实控人' AS relatedtype,
            '' AS trade_amount,
            ''AS isnew,
            '' AS iscancel,
            CASE WHEN b.bbd_qyxx_id IS NOT NULL THEN b.company_enterprise_status
                    WHEN c.bbd_qyxx_id IS NOT NULL THEN c.company_enterprise_status
                    ELSE '' END AS enterprise_status
        FROM part4_tmp a
        LEFT JOIN qyxx_basic b
        ON a.name=b.company_name
        LEFT JOIN qyxx_basic c
        ON a.name=c.history_name
        LEFT JOIN vertex_company_branch d
        ON a.name=d.name
        '''
    )

    part1.union(part2).union(part3).union(part4).createOrReplaceTempView('company')

    spark.sql(
        '''
        SELECT 
            tmp2.*
        FROM(
            SELECT 
                tmp1.*,
                row_number() over(distribute by tmp1.name order by tmp1.flag, tmp1.uid asc ) AS rank
            FROM(
                SELECT 
                    *,
                    if(locate('存续', enterprise_status) > 0, '1', '2') AS flag 
                FROM company
            ) tmp1
        ) tmp2
        WHERE tmp2.rank=1
        '''
    ).createOrReplaceTempView('company')

    company = spark.sql(
        '''
        SELECT
            CASE WHEN a.uid == 'null' OR a.uid IS NULL THEN '' ELSE a.uid END AS uid,
            CASE WHEN a.name == 'null' OR a.name IS NULL THEN '' ELSE a.name END AS name,
            CASE WHEN b.company_name == 'null' OR b.company_name IS NULL THEN '' ELSE '上市公司' END AS ipo,
            CASE WHEN a.type == 'null' OR a.type IS NULL THEN '' ELSE a.type END AS type,
            CASE WHEN a.company_industry == '' OR a.company_industry == 'null' OR a.company_industry IS NULL THEN '' 
                    ELSE c.value END AS industry,
            CASE WHEN a.form == 'null' OR a.form IS NULL THEN '' ELSE a.form END AS form,
            CASE WHEN a.openfrom == 'null' OR a.openfrom IS NULL THEN '' ELSE a.openfrom END AS openfrom,
            CASE WHEN a.opento == 'null' OR a.opento IS NULL THEN '' ELSE a.opento END AS opento,
            CASE WHEN a.range == 'null' OR a.range IS NULL THEN '' ELSE a.range END AS range,
            CASE WHEN a.regcapamount == 'null' OR a.regcapamount IS NULL THEN '' 
                ELSE a.regcapamount END AS regcapamount,
            CASE WHEN a.regcapcurrency == 'null' OR a.regcapcurrency IS NULL THEN '' 
                ELSE a.regcapcurrency END AS regcapcurrency,
            CASE WHEN a.relatedid == 'null' OR a.relatedid IS NULL THEN '' ELSE a.relatedid END AS relatedid,
            CASE WHEN a.nameid == 'null' OR a.nameid IS NULL THEN '' ELSE a.nameid END AS nameid,
            CASE WHEN a.islowcap == 'null' OR a.islowcap IS NULL THEN '' ELSE a.islowcap END AS islowcap,
            CASE WHEN a.branch == 'null' OR a.branch IS NULL THEN '' ELSE a.branch END AS branch,
            CASE WHEN a.history_name == 'null' OR a.history_name IS NULL THEN '' 
                ELSE a.history_name END AS history_name,
            CASE WHEN a.type == '' OR a.type == 'null' OR a.type IS NULL THEN '' 
                    ELSE d.company_type  END AS company_type,
            CASE WHEN a.relatedtype == 'null' OR a.relatedtype IS NULL THEN '' ELSE a.relatedtype END AS relatedtype,
            CASE WHEN a.trade_amount == 'null' OR a.trade_amount IS NULL THEN '' 
                ELSE a.trade_amount END AS trade_amount,
            CASE WHEN a.isnew == 'null' OR a.isnew IS NULL THEN '' ELSE a.isnew END AS isnew,
            CASE WHEN a.iscancel == 'null' OR a.iscancel IS NULL THEN '' ELSE a.iscancel END AS iscancel,
            CASE WHEN a.enterprise_status == 'null' OR a.enterprise_status IS NULL THEN '' 
                ELSE a.enterprise_status END AS enterprise_status
        FROM company a
        LEFT JOIN listed_name b
        ON a.name=b.company_name
        LEFT JOIN industry_map c 
        ON a.company_industry=c.key
        LEFT JOIN company_type d 
        ON a.type=d.type
        '''
    ).repartition(20)

    # 数据入MySQL和写HDFS
    args = {'MySQL': 'vertex_company', 'HDFS': path_dict['company_save_path']}
    save_result(entry, company, args)

    company.createOrReplaceTempView("vertex_company")
    logger.info(f"vertex_company table has been written to hdfs path {path_dict['company_save_path']}")
    logger.info("entity_module has generated vertex_company table")


def vertex_person(entry: Entry, df_dict: dict, path_dict: dict):
    """
    公司高管或投资人
    :param entry:
    :param df_dict:
    :param path_dict:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    version = entry.version

    bjxs = str(set(bjx)).replace('{', '(').replace('}', ')')

    logger.info("entity_module start generating vertex_person table")
    part1 = spark.sql(
        f'''
        SELECT 
            frname_id AS uid, 
            frname AS name, 
            'legal' AS type, 
            bbd_qyxx_id AS relatedid,
            '' AS nameid,
            '' AS ratio,
            '' AS amount,
            '' AS time
        FROM qyxx_basic
        WHERE frname IS NOT NULL 
        AND LENGTH(frname) < 4 
        AND SUBSTRING (frname, 1, 1) IN {bjxs}
        UNION 
        SELECT 
            name_id AS uid,
            name,
            position AS type,
            bbd_qyxx_id AS relatedid,
            '' AS nameid,
            '' AS ratio,
            '' AS amount,
            '' AS time
        FROM qyxx_baxx 
        WHERE name IS NOT NULL 
        AND LENGTH(name) < 4 
        AND SUBSTRING (name, 1, 1) IN {bjxs}
        UNION 
        SELECT
            frname_id AS uid,
            frname AS name,
            'pllegal' AS type,
            bbd_qyxx_id AS relatedid,
            '' AS nameid,
            '' AS ratio,
            '' AS amount,
            report_date AS time
        FROM listed_name
        WHERE frname_compid=1
        UNION 
        SELECT
            stock_code + MD5(customer_name) AS uid,
            customer_name AS name,
            'customer' AS type,
            bbd_qyxx_id AS relatedid,
            report_date AS nameid,
            '' AS ratio,
            trade_amount AS amount,
            report_date AS time
        FROM qyxx_ipo_customer
        WHERE customer_compid=1
        UNION 
        SELECT
            stock_code + MD5(supplier_name) AS uid,
            supplier_name AS name,
            'supplier' AS type,
            bbd_qyxx_id AS relatedid,
            report_date AS nameid,
            '' AS ratio,
            trade_amount AS amount,
            report_date AS time
        FROM qyxx_ipo_supplier
        WHERE supplier_compid=1
        '''
    )

    def get_date(paid_detail):
        json_list = json.loads(paid_detail)
        if json_list is None or len(json_list) == 0:
            return ''
        else:
            paid_date = sorted([paid_info.get("paid_date") if paid_info.get("paid_date") else '1970-01-01' for paid_info in json_list], reverse=True)[0]
            if paid_date == '1970-01-01':
                return ''
            str2date = datetime.datetime.strptime(version, "%Y%m")
            last_year = DateUtils.add_date_2str(date_time=str2date, fmt="%Y%m", years=-1)
            now_date = last_year[:4] + '-12-31'
            if 0 < len(re.findall("[\u4e00-\u9fa5]+", paid_date)):
                paid_date = paid_date.replace(u'年', '-').replace(u'月', '-').replace(u'日', '')
            paid_date_timestamp = time.mktime(time.strptime(paid_date, "%Y-%m-%d"))
            now_date_timestamp = time.mktime(time.strptime(now_date, "%Y-%m-%d"))
            timedelta = int(now_date_timestamp) - int(paid_date_timestamp)
            if 0 < timedelta:
                return paid_date
            else:
                return ''

    spark.udf.register('get_date', get_date)

    def name_filter(rows):
        rows = list(rows)
        re_list = list()
        for row in rows:
            name = row.name
            if name is None or name.isdigit() or name.isalpha() or u'伊利' in name or u'西门子' in name \
                    or u'海尔系' in name or u'屈臣氏' in name or u'华为' in name or u'艺龙网' in name or u'海信' in name \
                    or u'海尔' in name or u'富士康' in name or u'腾讯' in name:
                continue
            re_list.append(Row(uid=row.uid, name=row.name, type=row.type, relatedid=row.relatedid, nameid=row.nameid,
                               ratio=row.ratio, amount=row.amount, time=row.time))
        return iter(re_list)

    part2 = spark.sql(
        f'''
        SELECT
            shareholder_id AS uid,
            regexp_replace(shareholder_name, '[夫妇, 及其一致行动人]', '') AS name,
            'shareholder' AS type,
            bbd_qyxx_id AS relatedid,
            '' AS nameid,
            invest_ratio AS ratio,
            CASE WHEN invest_amount == '0' OR invest_amount == '0.0' OR invest_amount IS NULL THEN '' 
                    ELSE invest_amount END AS amount,
            get_date(paid_detail) as time
        FROM qyxx_gdxx
        WHERE shareholder_name IS NOT NULL
        AND LENGTH(shareholder_name) < 4
        AND SUBSTRING (shareholder_name, 1, 1) IN {bjxs}
        AND shareholder_name NOT RLIKE '等.*[人, 户人, 个自然人]'
        AND paid_detail IS NOT NULL
        '''
    ).rdd.mapPartitions(name_filter).toDF()

    def name_split(rows):
        rows = list(rows)
        re_list = list()
        for row in rows:
            names = row.ulcontroller.split('（')[0].split('、')
            for name in names:
                if 4 < len(name) and name[0] in bjx:
                    re_list.append(Row(code=row.code, name=name, bbd_qyxx_id=row.bbd_qyxx_id,
                                       report_date=row.report_date))
        return iter(re_list)

    spark.sql(
        '''
        SELECT
            code,
            ulcontroller,
            bbd_qyxx_id,
            report_date
        FROM listed_name
        '''
    ).rdd.mapPartitions(name_split).toDF().createOrReplaceTempView("part3_tmp")
    part3 = spark.sql(
        '''
        SELECT
            code + MD5(name),
            name,
            'actualcontrol' AS type,
            bbd_qyxx_id AS relatedid,
            '' AS nameid,
            '' AS ratio,
            '' AS amount,
            report_date as time
        FROM part3_tmp
        '''
    )

    part1.union(part2).union(part3).createOrReplaceTempView("union_person")

    person = spark.sql(
        '''
        SELECT
            CASE WHEN b.uuid IS NOT NULL THEN b.uuid ELSE a.uid END AS uid,
            a.name,
            CASE WHEN a.type == 'null' OR a.type IS NULL THEN '' ELSE a.type END AS type,
            CASE WHEN a.relatedid == 'null' OR a.relatedid IS NULL THEN '' ELSE a.relatedid END AS relatedid,
            CASE WHEN a.nameid == 'null' OR a.nameid IS NULL THEN '' ELSE a.nameid END AS nameid,
            CASE WHEN a.ratio == 'null' OR a.ratio IS NULL THEN '' ELSE a.ratio END AS ratio,
            CASE WHEN a.amount == 'null' OR a.amount IS NULL THEN '' ELSE a.amount END AS amount,
            CASE WHEN a.time == 'null' OR a.time IS NULL THEN '' ELSE a.time END AS time
        FROM(
            SELECT
                max(uid) over(distribute by name, relatedid) AS uid,
                name,
                type,
                relatedid,
                nameid,
                ratio,
                amount,
                time
            FROM union_person
        ) a
        LEFT JOIN listed_person_unique b
        ON a.name=b.name AND a.relatedid=b.bbd_qyxx_id
        '''
    ).distinct().repartition(450)

    # 数据入MySQL和写HDFS
    args = {'MySQL': 'vertex_person', 'HDFS': path_dict['person_save_path']}
    save_result(entry, person, args)

    df_dict['listed_name'].unpersist()
    logger.info(f"vertex_person table has been written to hdfs path {path_dict['person_save_path']}")
    logger.info("entity_module has generated vertex_person table")


def vertex_address(entry: Entry, path_dict: dict):
    """
    地址
    :param entry:
    :param path_dict:
    :return:
    """
    spark = entry.spark
    logger = entry.logger

    logger.info("entity_module start generating vertex_address table")
    pro_list = spark.sql(
        '''
        SELECT 
            prvc_describ,
            prvc 
        FROM province
        '''
    ).rdd.map(lambda x: (x.prvc_describ, x.prvc)).collect()
    city_list = spark.sql(
        '''
        SELECT 
            city_describ,
            city 
        FROM city
        '''
    ).rdd.map(lambda x: (x.city_describ, x.city)).collect()

    def address_clean(row):
        detail = row.detail
        detail = detail.replace(u'北京市北京市', u'北京市').replace(u'上海市上海市', u'上海市') \
            .replace(u'重庆市重庆市', u'重庆市').replace(u'天津市天津市', u'天津市') \
            .replace(u'东莞市东莞市', u'东莞市').replace(u'香港特别行政区香港特别行政区', u'香港特别行政区') \
            .replace(u'澳门特别行政区澳门特别行政区', u'澳门特别行政区')
        for pro in pro_list:
            if pro[0] in detail and pro[1] not in detail:
                detail = detail.replace(pro[0], pro[1])
        for cit in city_list:
            if cit[0] in detail and cit[1] not in detail:
                detail = detail.replace(cit[0], cit[1])
        return Row(detail=detail, relatedid=row.relatedid)

    spark.sql(
        '''
        SELECT 
            l.address AS detail,
            l.bbd_qyxx_id AS relatedid
        FROM qyxx_basic l 
        JOIN vertex_company r 
        ON l.address IS NOT NULL AND l.bbd_qyxx_id=r.uid
        '''
    ).rdd.map(address_clean).toDF().createOrReplaceTempView("address")

    address = spark.sql(
        '''
        SELECT 
            MD5(detail) AS uid,
            detail,
            relatedid 
        from address
        '''
    ).repartition(4)

    # 数据入MySQL和写HDFS
    args = {'MySQL': 'vertex_address', 'HDFS': path_dict['address_save_path']}
    save_result(entry, address, args)

    logger.info(f"vertex_address table has been written to hdfs path {path_dict['address_save_path']}")
    logger.info("entity_module has generated vertex_address table")


def phonenumber_filter(data):
    uid = data.uid
    uid = re.sub(r'\+86-?', '', uid)
    uid = re.sub(r'\-{2,}', '-', uid)
    s1 = uid.count('-')
    if s1 >= 2:
       parts = uid.split('-')
       uid = parts[0] + parts[1]
    else:
        uid = uid

    uid = re.findall('[0-9]+', uid)
    uid = ''.join(uid)
    if uid.startswith('86') and len(uid) > 10:
        uid = uid[2:]
    else:
        uid = uid

    if uid.startswith('0'):
        if len(uid) < 11 or len(uid) > 15:
            uid = ''
    else:
        if len(uid) < 7 or len(uid) > 15:
            uid = ''
    d_list = []
    for i in range(len(uid)):
        if i < len(uid)-1 and uid[i] == uid[i+1]:
            continue
        else:
            d_list.extend(uid[i])
    if len(d_list) == 1:
        uid = ''
    if '12345678' in uid or '87654321' in uid:
        uid = ''
    return Row(uid=uid, year=data.year, relatedid=data.relatedid)


def vertex_phonenumber(entry: Entry, path_dict: dict):
    """
    联系电话
    :param entry:
    :param path_dict:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    version = entry.version
    current_year = version[:4]

    logger.info("entity_module start generating vertex_phonenumber table")
    phonenumber = spark.sql(
        f'''
        SELECT 
            regexp_extract(l.phone, '[0-9\-+]+', 0) AS uid,
            l.year,
            l.bbd_qyxx_id AS relatedid
        FROM qyxx_annual_report_jbxx l 
        JOIN listed_relation_company r 
        ON l.bbd_qyxx_id=r.bbd_qyxx_id 
        WHERE l.phone IS NOT NULL 
        AND l.phone != ''
        AND l.phone != 'null'
        AND l.phone not regexp '000000'
        AND l.year < {current_year}
        '''
    ).where("length(uid) >= 6").rdd\
        .map(phonenumber_filter).toDF()\
        .where("uid != ''")\
        .repartition(6)

    # 数据入MySQL和写HDFS
    args = {'MySQL': 'vertex_phonenumber', 'HDFS': path_dict['phonenumber_save_path']}
    save_result(entry, phonenumber, args)

    logger.info(f"vertex_phonenumber table has been written to hdfs path {path_dict['phonenumber_save_path']}")
    logger.info("entity_module has generated vertex_phonenumber table")


def vertex_email(entry: Entry, path_dict: dict):
    spark = entry.spark
    logger = entry.logger
    version = entry.version
    current_year = version[:4]

    logger.info("entity_module start generating vertex_email table")
    email = spark.sql(
        f'''
        SELECT 
            email AS uid,
            year,
            bbd_qyxx_id AS relatedid
        FROM qyxx_annual_report_jbxx 
        WHERE email IS NOT NULL AND email != 'null' AND email != '' AND year < {current_year} AND length(email) > 8 
        AND email NOT regexp '^[0-9]+$' AND email NOT regexp '[\\u4e00-\\u9fa5]' AND email NOT regexp '^0000@' 
        AND substring(email, length(email) - 1)!='@' AND year < {current_year}
        '''
    ).repartition(100)

    # 数据入MySQL和写HDFS
    args = {'MySQL': 'vertex_email', 'HDFS': path_dict['email_save_path']}
    save_result(entry, email, args)

    logger.info(f"vertex_email table has been written to hdfs path {path_dict['email_save_path']}")
    logger.info("entity_module has generated vertex_email table")


def vertex_orgcode(entry: Entry, df_dict: dict, path_dict: dict):
    """
    组织机构
    :param entry:
    :param df_dict:
    :param path_dict:
    :return:
    """
    spark = entry.spark
    logger = entry.logger

    logger.info("entity_module start generating vertex_orgcode table")
    orgcode = spark.sql(
        '''
        SELECT 
            substring(regexp_replace(credit_code, '-', ''), 9, 9) as uid
        FROM qyxx_basic
        '''
    ).distinct().repartition(1)

    # 数据入MySQL和写HDFS
    args = {'MySQL': 'vertex_orgcode', 'HDFS': path_dict['orgcode_save_path']}
    save_result(entry, orgcode, args)

    df_dict['qyxx_basic'].unpersist()
    logger.info(f"vertex_orgcode table has been written to hdfs path {path_dict['orgcode_save_path']}")
    logger.info("entity_module has generated vertex_orgcode table")


def punish_filter(row):
    punishcontent = row.punishcontent

    pattern_chinese = re.compile(r'[\u4e00-\u9fa5]+')
    pattern_number = re.compile(r'\d+')
    if punishcontent is not None:
        if len(re.findall(pattern_chinese, punishcontent)) == 0 \
            or len(re.findall(pattern_number, punishcontent)) == 0 \
            or punishcontent == 'null' \
            or punishcontent == u'无' or punishcontent == u'空' or punishcontent == u'暂未入库' \
            or punishcontent == u'未知' or punishcontent == u'已入库' or punishcontent == u'无内容' \
            or punishcontent == u'无内容' or punishcontent == u'同意。' or punishcontent == u'圆满' \
            or punishcontent == u'报送中' or punishcontent == u'见附件' or punishcontent == u'暂缺' \
            or punishcontent == u'"一、' or punishcontent == u'"1、' or punishcontent == u'1:其他;' \
                or len(punishcontent) < 10:
            punishcontent = u'暂无详情'
    else:
        punishcontent = u'暂无详情'

    return Row(uid=row.uid, punishdate=row.punishdate, punishcategory=row.punishcategory,
               punishcontent=punishcontent, relatedid=row.relatedid, punishcode=row.punishcode)


def vertex_punish(entry: Entry, path_dict: dict):
    """
    处罚
    :param entry:
    :param path_dict:
    :return:
    """
    spark = entry.spark
    logger = entry.logger

    logger.info("entity_module start generating vertex_punish table")
    punish = spark.sql(
        '''
        SELECT 
            a.uid,
            CASE WHEN a.punishdate == 'null' OR a.punishdate IS NULL THEN '' ELSE a.punishdate END AS punishdate,
            CASE WHEN a.punishcategory IS NULL THEN '' ELSE a.punishcategory END AS punishcategory,
            CASE WHEN a.punishcontent == 'null' OR a.punishcontent IS NULL THEN '' ELSE a.punishcontent END AS punishcontent,
            CASE WHEN a.relatedid == 'null' THEN '' ELSE a.relatedid END AS relatedid,
            CASE WHEN a.punishcode == 'null' OR a.punishcode IS NULL THEN '' ELSE a.punishcode END AS punishcode
        FROM(
            SELECT 
                concat(bbd_unique_id, '_cf') AS uid,
                punish_date AS punishdate,
                if(punish_type='null', '行政处罚', punish_type) AS punishcategory,
                punish_content AS punishcontent,
                ifnull(bbd_qyxx_id, '') AS relatedid, 
                punish_code AS punishcode 
            FROM qyxx_xzcf 
            UNION 
            SELECT 
                concat(bbd_xgxx_id, '_cf') AS uid,
                public_date AS punishdate,
                if(punish_category_one='null', '行政处罚', punish_category_one) AS punishcategory,
                punish_content AS punishcontent,
                ifnull(bbd_qyxx_id, '') AS relatedid, 
                punish_code AS punishcode 
            FROM xzcf
            UNION 
            SELECT 
                concat(bbd_xgxx_id, '_cf') AS uid,
                pubdate AS punishdate,
                if(type='null', '行政处罚', type) AS punishcategory,
                main AS punishcontent,
                ifnull(bbd_qyxx_id, '') AS relatedid, 
                '暂无详情' AS punishcode 
            FROM qyxg_circxzcf 
        ) a
        '''
    ).rdd.map(punish_filter).toDF()\
        .dropDuplicates(["punishdate", "punishcode", "relatedid"])\
        .repartition(20)

    # 数据入MySQL和写HDFS
    args = {'MySQL': 'vertex_punish', 'HDFS': path_dict['punish_save_path']}
    save_result(entry, punish, args)

    logger.info(f"vertex_punish table has been written to hdfs path {path_dict['punish_save_path']}")
    logger.info("entity_module has generated vertex_punish table")


def judicial_filter(data):
    cause_ori_1 = data.cause
    cause_ori_2 = re.sub(r"<.*>", '', cause_ori_1)
    cause = re.sub(r"\s", '', cause_ori_2)
    pattern_chinese = re.compile(r'[\u4e00-\u9fa5]+')
    pattern_number = re.compile(r'\d+')
    if cause is not None:
        if data.flag == u'0':
            if ((cause.endswith(u'法庭') or cause.endswith(u'法院') or cause.endswith(u'审判庭')) and len(cause) <= 30) \
                    or len(cause) <= 15 or (len(re.findall(pattern_chinese, cause))) == 0:
                cause = u'暂无详情'
            else:
                cause = cause
        elif data.flag == u'1':
            if ((cause.endswith(u'法庭') or cause.endswith(u'法院') or cause.endswith(u'审判庭')) and len(cause) <= 30) \
                    or len(cause) <= 15 or len(re.findall(pattern_chinese, cause)) == 0 \
                    or len(re.findall(pattern_number, cause)) == 0:
                cause = u'暂无详情'
            else:
                cause = cause
        elif data.flag == u'2':
            if ((cause.endswith(u'法庭') or cause.endswith(u'法院') or cause.endswith(u'审判庭')) and len(cause) <= 10) \
                    or len(cause) <= 15 or len(re.findall(pattern_chinese, cause)) == 0 \
                    or len(re.findall(pattern_number, cause)) == 0:
                cause = u'暂无详情'
            else:
                cause = cause
        elif data.flag == u'3':
            if ((cause.endswith(u'法庭') or cause.endswith(u'法院') or cause.endswith(u'审判庭')) and len(cause) <= 10) \
                    or len(cause) <= 5 or len(re.findall(pattern_chinese, cause)) == 0 \
                    or len(re.findall(pattern_number, cause)) == 0:
                cause = u'暂无详情'
            else:
                cause = cause

    return Row(uid=data.uid, type=data.type, date=data.date, relatedid=data.relatedid, cause=cause,
               accuser=data.accuser, defendant=data.defendant, casetype=data.casetype)


def vertex_judicial(entry: Entry, df_dict: dict, path_dict: dict):
    """
    司法
    :param entry:
    :param df_dict:
    :param path_dict:
    :return:
    """
    spark = entry.spark
    logger = entry.logger

    logger.info("entity_module start generating vertex_judicial table")

    part1 = spark.sql(
        '''
        SELECT
            concat(bbd_xgxx_id, '_sf') AS uid,
            if(notice_type='null', '处罚公告', notice_type) AS type,
            notice_time AS date,
            bbd_qyxx_id AS relatedid,
            notice_content AS cause,
            accuser,
            defendant,
            case_type AS casetype,
            '0' AS flag
        FROM legal_court_notice
        '''
    ).rdd.map(judicial_filter).toDF()\
        .dropDuplicates(["type", "date", "relatedid", "cause", "accuser", "defendant", "casetype"]).rdd \
        .map(lambda row: (row.uid, row)) \
        .repartition(2) \
        .map(lambda row: row[1]).toDF()

    part2 = spark.sql(
        '''
        SELECT
            concat(bbd_xgxx_id, '_sf') AS uid,
            if(action_cause='null', '开庭公告', action_cause) AS type,
            trial_date AS date,
            bbd_qyxx_id AS relatedid,
            main AS cause,
            accuser,
            defendant,
            case_type AS casetype,
            '1' AS flag
        FROM ktgg
        '''
    ).rdd.map(judicial_filter).toDF()\
        .dropDuplicates(["type", "date", "relatedid", "cause", "accuser", "defendant", "casetype"])\
        .repartition(1)

    def get_accuser(name):
        """
        UDF
        :param name:
        :return:
        """
        try:
            josn_str = json.loads(name, encoding='UTF-8')
            result = []
            for key in josn_str:
                if josn_str.get(key) == u'原告':
                    result.append(key)
            return ','.join(result)
        except:
            return ''

    def get_defendant(name):
        """
        UDF
        :param name:
        :return:
        """
        try:
            josn_str = json.loads(name, encoding='UTF-8')
            result = []
            for key in josn_str:
                if josn_str.get(key) == u'被告':
                    result.append(key)
            return ','.join(result)
        except:
            return ''

    spark.udf.register('get_accuser', get_accuser)
    spark.udf.register('get_defendant', get_defendant)

    part3 = spark.sql(
        '''
        SELECT 
            concat(bbd_xgxx_id, '_sf') AS uid,
            action_cause AS type,
            sentence_date AS date,
            bbd_qyxx_id AS relatedid,
            notice_content AS cause,
            if(litigation_status is null, '', get_accuser(litigation_status)) AS accuser,
            if(litigation_status is null, '', get_defendant(litigation_status)) AS defendant,
            case_type AS casetype,
            '2' AS flag 
        FROM legal_adjudicative_documents
        '''
    ).rdd.map(judicial_filter).toDF()\
        .dropDuplicates(["type", "date", "relatedid", "cause", "accuser", "defendant", "casetype"]).rdd\
        .map(lambda row: (row.uid, row))\
        .repartition(150)\
        .map(lambda row: row[1]).toDF()

    part4 = spark.sql(
        '''
        SELECT
            concat(bbd_xgxx_id, '_sf') AS uid,
            '被执行人' AS type,
            register_date AS date,
            bbd_qyxx_id AS relatedid,
            concat(exec_court_name, case_code) AS cause,
            '' AS accuser,
            pname AS defendant,
            '' AS casetype,
            '3' AS flag
        FROM legal_persons_subject_to_enforcement
        '''
    ).rdd.map(judicial_filter).toDF()\
        .dropDuplicates(["type", "date", "relatedid", "cause", "accuser", "defendant", "casetype"])\
        .repartition(1)

    judicial = part1.union(part2).union(part3).union(part4)\
        .dropDuplicates(["type", "date", "relatedid", "cause", "accuser", "defendant", "casetype"]).rdd\
        .map(lambda row: (row.uid, row))\
        .repartition(150)\
        .map(lambda row: row[1]).toDF()

    # 数据入MySQL和写HDFS
    args = {'MySQL': 'vertex_judicial', 'HDFS': path_dict['judicial_save_path']}
    save_result(entry, judicial, args)

    df_dict['listed_relation_company'].unpersist()
    logger.info(f"vertex_judicial table has been written to hdfs path {path_dict['judicial_save_path']}")
    logger.info("entity_module has generated vertex_judicial table")


def bfat_filter(data):
    cause_ori_1 = data.cause
    cause_ori_2 = re.sub(r"<.*>", '', cause_ori_1)
    cause = re.sub(r"\s", '', cause_ori_2)
    pattern_chinese = re.compile(r'[\u4e00-\u9fa5]+')
    pattern_english = re.compile(r'[a-zA-Z]+')
    pattern_number = re.compile(r'\d+')
    if cause is not None:
        if len(cause) <= 10 or len(re.findall(pattern_english, cause)) == 0 \
                or len(re.findall(pattern_number, cause)) == 0 \
                or len(re.findall(pattern_chinese, cause)) == 0:
            cause = u'暂无详情'
        else:
            cause = cause
    else:
        cause = u'暂无详情'

    return Row(uid=data.uid, type=data.type, date=data.date, performdegree=data.performdegree, relatedid=data.relatedid,
               cause=cause, code=data.code)


def vertex_bfat(entry: Entry, df_dict: dict, path_dict: dict):
    """
    失信名单
    :param entry:
    :param df_dict:
    :param path_dict:
    :return:
    """
    spark = entry.spark
    logger = entry.logger

    logger.info("entity_module start generating vertex_bfat table")
    bfat = spark.sql(
        '''
        SELECT  
            concat(bbd_xgxx_id, '_sx') AS uid,
            if(dishonest_situation='null', '失信', dishonest_situation) AS type,
            register_date AS date,
            coalesce(exec_state, '') AS performdegree,
            bbd_qyxx_id AS relatedid,
            exec_obligation AS cause,
            case_code AS code
        FROM legal_dishonest_persons_subject_to_enforcement
        '''
    ).rdd.map(bfat_filter).toDF()\
        .dropDuplicates(["type", "date", "performdegree", "relatedid", "cause", "code"])\
        .repartition(50)

    # 数据入MySQL和写HDFS
    args = {'MySQL': 'vertex_bfat', 'HDFS': path_dict['bfat_save_path']}
    save_result(entry, bfat, args)

    df_dict['listed_name_qyxx_id'].unpersist()
    logger.info(f"vertex_bfat table has been written to hdfs path {path_dict['bfat_save_path']}")
    logger.info("entity_module has generated vertex_bfat table")


def entity_calculate(entry: Entry, df_dict: dict, path_dict: dict):
    """
    计算实体
    :param entry:
    :param df_dict:
    :param path_dict:
    :return:
    """
    logger = entry.logger
    logger.info("entity_module start to calculate entity")

    # 解析qyxx_ipo_customer和qyxx_ipo_supplier
    qyxx_ipo_customer_and_supplier(entry, df_dict, path_dict)
    # 分子公司
    vertex_company_branch(entry, path_dict)
    # 公司
    vertex_company(entry, path_dict)
    # 人
    vertex_person(entry, df_dict, path_dict)
    # 地址
    vertex_address(entry, path_dict)
    # 电话
    vertex_phonenumber(entry, path_dict)
    # 邮箱
    vertex_email(entry, path_dict)
    # 组织机构代码
    vertex_orgcode(entry, df_dict, path_dict)
    # 处罚
    vertex_punish(entry, path_dict)
    # 司法
    vertex_judicial(entry, df_dict, path_dict)
    # 失信名单
    vertex_bfat(entry, df_dict, path_dict)
    logger.info("entity_module has calculated entity")


def main(entry: Entry):
    # 数据存储路径
    path_dict = data_path(entry)
    # 抽取基础数据
    df_dict = extract_basic_data(entry, path_dict)
    # 实体计算
    entity_calculate(entry, df_dict, path_dict)


def post_check(entry: Entry):
    pass

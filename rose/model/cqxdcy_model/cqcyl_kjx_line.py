#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created On Tus 2 Mar 2021

@author john

Email: meijian@bbdservice.com

Instruction: 重庆产业链项目科技型企业
"""

import pandas as pd
import copy
import json
import time
import warnings
warnings.filterwarnings("ignore")

#####part1.数据获取及处理
def read_data(kjx_path):
    """
    读取招商价值指标文件，按行业划分
    :param kjx_path: 招商价值文件路径
    :return:
    """
    data_kjx = pd.read_csv(kjx_path,sep='\t')
    try:
        data_kjx = data_kjx.loc[data_kjx['industry']!='industry',:]
    except:
        pass
    data_kjx = data_kjx.drop(['company_name', 'province', 'city', 'district','sub_industry'],axis=1)
    data_kjx_xcl = data_kjx.loc[data_kjx['industry'] == '新材料',:]
    data_kjx_xny = data_kjx.loc[data_kjx['industry'] == '新能源',:]
    data_kjx_jcdl = data_kjx.loc[data_kjx['industry'] == '集成电路',:]

    #指标数据存在重复数据，也可能是一个企业对应多产业，所有这里分产业之后进行去重
    data_kjx_xcl = data_kjx_xcl.drop_duplicates('bbd_qyxx_id')
    data_kjx_xny = data_kjx_xny.drop_duplicates('bbd_qyxx_id')
    data_kjx_jcdl = data_kjx_jcdl.drop_duplicates('bbd_qyxx_id')

    return (data_kjx_xcl,data_kjx_xny,data_kjx_jcdl)

def data_deal(data_indus):
    """
    数据处理模块，每个行业进行同样处理
    :param data_indus:
    :return:
    """
    # 无离散型指标
    # 连续型指标
    for col in list(data_indus.columns)[2:]:
        data_indus[col] = data_indus[col].replace('--',0)
        data_indus[col] = data_indus[col].astype(float)
        data_indus[col] = data_indus[col].fillna(data_indus[col].mean())

    return data_indus

#######part2.分值评价输出
def singal_index_score(data_indus):
    """
    对单个的指标按照离散、连续分别输出指标评分
    :param data_indus: 处理后的指标数据
    :return:
    """
    data_indus = copy.deepcopy(data_indus)
    # 连续型指标
    def max_min_trans(x):
        "最大最小归一化得分"
        if x.max() == x.min():
            return 50
        else:
            return (x - x.min()) / (x.max() - x.min()) * 99

    continue_list = list(data_indus.columns)[2:]
    for col in continue_list:
        data_indus[col+'_score'] = max_min_trans(data_indus[col])

    # 离散型指标S1：无
    # '负向指标，进行转向：无'

    return data_indus


def output_mode_score(data_indus, weight_path):
    """
    按照权重参数文件输出各个二级指标以及一级模块得分and科技型成长性企业总分
    :param data_indus:指标得分结果表
    :return:
    """
    data_indus = copy.deepcopy(data_indus)

    with open(weight_path, 'r') as f:
        weight_dict = json.load(f)

    # 由于科技型成长性企业不需要输出二级模块得分，所以简化处理，直接根据指标得分计算一级模块得分

    # 核心技术情况
    hxjsqk_score_list = ['V1_score', 'V2_score', 'V3_score', 'V4_score',
                         '460C_score', 'V6_1_score', 'V6_2_score', '306C_score']

    # 研发能力情况
    yfnlqk_score_list = ['52C_score', '53C_score', '187C_score', 'V13_score', 'V14_score']

    # 研发成果市场认可情况
    yfscrkqk_score_list = ['V16_score', 'V17_score']

    # 相对竞争优势情况
    xdjzysqk_score_list = ['V18_score', 'V19_score', 'V21_score', '444C_score']

    # 技术成果转化情况
    jscgzhqk_score_list = ['V24_score', 'V26_score', 'V27_score', 'V28_score']

    # 所有指标按照excel顺序排列
    all_index_list = hxjsqk_score_list + yfnlqk_score_list + yfscrkqk_score_list \
                     + xdjzysqk_score_list + jscgzhqk_score_list
    # 各个指标对应计算模块得分的权重
    index_all_weight = [0.1, 0.1, 0.1, 0.1, 0.2, 0.14, 0.14, 0.12,
                        0.15, 0.15, 0.15, 0.25, 0.3,
                        0.5, 0.5,
                        0.3, 0.3, 0.2, 0.2,
                        0.25, 0.25, 0.25, 0.25]
    all_index_weight_dict = {}
    for i in range(len(all_index_list)):
        all_index_weight_dict[all_index_list[i]] = index_all_weight[i]

    # 计算各个一级模块得分
    data_indus.loc[:, 'hxjsqk_score'] = data_indus['V1_score'] * all_index_weight_dict[hxjsqk_score_list[0]] \
                                        + data_indus['V2_score'] * all_index_weight_dict[hxjsqk_score_list[1]] \
                                        + data_indus['V3_score'] * all_index_weight_dict[hxjsqk_score_list[2]] \
                                        + data_indus['V4_score'] * all_index_weight_dict[hxjsqk_score_list[3]] \
                                        + data_indus['460C_score'] * all_index_weight_dict[hxjsqk_score_list[4]] \
                                        + data_indus['V6_1_score'] * all_index_weight_dict[hxjsqk_score_list[5]] \
                                        + data_indus['V6_2_score'] * all_index_weight_dict[hxjsqk_score_list[6]] \
                                        + data_indus['306C_score'] * all_index_weight_dict[hxjsqk_score_list[7]]

    data_indus.loc[:, 'yfnlqk_score'] = data_indus['52C_score'] * all_index_weight_dict[yfnlqk_score_list[0]] \
                                        + data_indus['53C_score'] * all_index_weight_dict[yfnlqk_score_list[1]] \
                                        + data_indus['187C_score'] * all_index_weight_dict[yfnlqk_score_list[2]] \
                                        + data_indus['V13_score'] * all_index_weight_dict[yfnlqk_score_list[3]] \
                                        + data_indus['V14_score'] * all_index_weight_dict[yfnlqk_score_list[4]]

    data_indus.loc[:, 'yfscrkqk_score'] = data_indus['V16_score'] * all_index_weight_dict[yfscrkqk_score_list[0]] \
                                          + data_indus['V17_score'] * all_index_weight_dict[yfscrkqk_score_list[1]]

    data_indus.loc[:, 'xdjzysqk_score'] = data_indus['V18_score'] * all_index_weight_dict[xdjzysqk_score_list[0]] \
                                          + data_indus['V19_score'] * all_index_weight_dict[xdjzysqk_score_list[1]] \
                                          + data_indus['V21_score'] * all_index_weight_dict[xdjzysqk_score_list[2]] \
                                          + data_indus['444C_score'] * all_index_weight_dict[xdjzysqk_score_list[3]]

    data_indus.loc[:, 'jscgzhqk_score'] = data_indus['V24_score'] * all_index_weight_dict[jscgzhqk_score_list[0]] \
                                          + data_indus['V26_score'] * all_index_weight_dict[jscgzhqk_score_list[1]] \
                                          + data_indus['V27_score'] * all_index_weight_dict[jscgzhqk_score_list[2]] \
                                          + data_indus['V28_score'] * all_index_weight_dict[jscgzhqk_score_list[3]]

    # 将一级模块的分数放缩到99
    def max_min_trans(x):
        "二级指标最大最小归一化"
        if x.max() == x.min():
            return 50
        else:
            return (x - x.min()) / (x.max() - x.min()) * 99

    first_class_mode_name = ['hxjsqk_score', 'yfnlqk_score', 'yfscrkqk_score',
                             'xdjzysqk_score', 'jscgzhqk_score']

    for col in first_class_mode_name:
        data_indus[col] = max_min_trans(data_indus[col])

    # 计算科技型得分
    data_indus.loc[:, 'kjx_score'] = data_indus['hxjsqk_score'] * weight_dict['fisrt_class_weight']['kjxczxqypf'][
        'hxjsqk_score'] \
                                     + data_indus['yfnlqk_score'] * weight_dict['fisrt_class_weight']['kjxczxqypf'][
                                         'yfnlqk_score'] \
                                     + data_indus['yfscrkqk_score'] * weight_dict['fisrt_class_weight']['kjxczxqypf'][
                                         'yfscrkqk_score'] \
                                     + data_indus['xdjzysqk_score'] * weight_dict['fisrt_class_weight']['kjxczxqypf'][
                                         'xdjzysqk_score'] \
                                     + data_indus['jscgzhqk_score'] * weight_dict['fisrt_class_weight']['kjxczxqypf'][
                                         'jscgzhqk_score']

    return data_indus

def output_result(result_xcl,result_xny,result_lcdl,output_path):
    """
    :param result_xcl:
    :param result_xny:
    :param result_lcdl: 3个产业的结果表
    :param output_path: 结果输出路径
    :return:
    """
    def result_deal(data):
        # 选出需要的列
        data = copy.deepcopy(data)
        first_class_mode_name = ['hxjsqk_score', 'yfnlqk_score', 'yfscrkqk_score',
                                 'xdjzysqk_score', 'jscgzhqk_score','kjx_score']
        output_col = ['bbd_qyxx_id','industry']+first_class_mode_name
        data_ = data.loc[:,output_col]
        # 行业内部排序
        data__ = data_.sort_values(by=['kjx_score'],ascending=False)
        return data__

    result_xcl_ = result_deal(result_xcl)
    result_xny_ = result_deal(result_xny)
    result_lcdl_ = result_deal(result_lcdl)

    kjx_result = pd.concat([result_xcl_,
                              result_xny_,
                              result_lcdl_])

    # 输出结果
    kjx_result.to_csv(output_path,index=False,encoding='utf-8')
    return kjx_result

def main(kjx_path,weight_path,output_path):
    """
    计算招商价值结果主函数
    :param kjx_path: 招商价值指标文件路径
    :param weight_path: 权重表路径
    :param output_path: 结果输出路径
    :return:
    """
    print('读数')
    data_kjx_xcl, data_kjx_xny, data_kjx_jcdl = read_data(kjx_path)

    print('数据处理')
    data_kjx_xcl_deal = data_deal(data_kjx_xcl)
    data_kjx_xny_deal = data_deal(data_kjx_xny)
    data_kjx_jcdl_deal = data_deal(data_kjx_jcdl)

    print('指标得分')
    kjx_xcl_index_score = singal_index_score(data_kjx_xcl_deal)
    kjx_xny_index_score = singal_index_score(data_kjx_xny_deal)
    kjx_jcdl_index_score = singal_index_score(data_kjx_jcdl_deal)

    print('模块得分，总分')
    kjx_xcl_result = output_mode_score(kjx_xcl_index_score, weight_path)
    kjx_xny_result = output_mode_score(kjx_xny_index_score, weight_path)
    kjx_jcdl_result = output_mode_score(kjx_jcdl_index_score, weight_path)

    print('结果融合')
    kjx_result = output_result(kjx_xcl_result, kjx_xny_result, kjx_jcdl_result, output_path)

    return kjx_result


if __name__ == "__main__":
    kjx_path = './kjx.csv'
    weight_path = './weight.json'
    output_path = './kjx_result.csv'

    kjx_result = main(kjx_path, weight_path, output_path)







#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created On Thu 2 Dec 2021

@author john

Email: meijian@bbdservice.com

Instruction: 重庆产业链函数库
"""
import pandas as pd
import copy
import time
import json


def read_data(file_path):
    """
    读取文件，并按照所有的产业进行分类
    :param file_path: 本地文件路径
    :return:
    """
    data_ = pd.read_csv(file_path, sep='\t')
    try:
        data_ = data_.loc[data_['industry'] != 'industry', :]
    except:
        pass
    # data_ = data_.drop(['company_name', 'province', 'city', 'district', 'sub_industry'], axis=1)
    data_ = data_.drop(['company_name', 'province', 'city', 'district'], axis=1)
    data_ = data_.drop_duplicates('bbd_qyxx_id')

    return data_

    # data__xcl = data_.loc[data_['industry'] == '新材料', :]
    # data__xny = data_.loc[data_['industry'] == '新能源', :]
    # data__infotech = data_.loc[data_['industry'] == '新一代信息技术', :]
    #
    # # 指标数据存在重复数据，也可能是一个企业对应多产业，所有这里分产业之后进行去重
    # data_list = [data__xcl, data__xny, data__infotech]
    # for i in range(len(data_list)):
    #     data_list[i] = data_list[i].drop_duplicates('bbd_qyxx_id')
    #
    # return (data__xcl, data__xny, data__infotech)


def data_deal(data_indus, discrete_index):
    """
    数据处理模块，每个行业进行同样处理
    :param data_indus:
    :param discrete_index:离散型指标列表--list
    :return:
    """

    # 离散型指标
    if len(discrete_index) > 0:
        for col in discrete_index:
            data_indus[col] = data_indus[col].fillna('B')
    # 连续型指标
    # for col in list(data_indus.columns)[2:]:
    for col in list(data_indus.columns)[3:]:
        if col not in discrete_index:
            # print(col)
            data_indus[col] = data_indus[col].replace('--', 0)
            data_indus[col] = data_indus[col].astype(float)
            data_indus[col] = data_indus[col].fillna(data_indus[col].mean())

    return data_indus


# 指标分值输出
def single_index_score(data_indus, discrete_index, negative_index):
    """
    对单个的指标按照离散、连续分别输出指标评分
    :param data_indus: 处理后的指标数据
    :param discrete_index: 离散型指标列表--list
    :param negative_index: 负向指标列表--list
    :return:
    """

    data_indus = copy.deepcopy(data_indus)
    # # Chen added
    # companies = df_top_companies['bbd_qyxx_id'].tolist()
    # df_force = data_indus[data_indus['bbd_qyxx_id'].isin(companies)]

    # 连续型指标
    def max_min_trans(x):
        "最大最小归一化得分"
        if x.max() == x.min():
            return 50
        else:
            return (x - x.min()) / (x.max() - x.min()) * 99

    # continue_list = list(data_indus.columns)[2:]
    continue_list = list(data_indus.columns)[3:]
    for col in continue_list:
        # print(col)
        if col not in discrete_index:
            data_indus[col + '_score'] = max_min_trans(data_indus[col])
    #         # 定向加分 Chen added
    #         max_value = data_indus[col + '_score'].max()
    #         df_force[col + '_score'] = max_value * (1 - df_top_companies['rank'] / 100).values
    # data_indus.update(df_force)

    # 离散型指标K22
    index_K22_dict = {'A': 99, 'B': 60}
    if len(discrete_index) > 0:
        for col in discrete_index:
            data_indus[col + '_score'] = data_indus[col].apply(lambda x: index_K22_dict[x])

    # Add by meijian on 20210302
    # 'K23为负向指标，进行转向'
    if len(negative_index) > 0:
        for col in negative_index:
            data_indus[col + '_score'] = data_indus[col + '_score'].apply(lambda x: 99 - x)
        #     # 定向加分 Chen added
        #     max_value = data_indus[col + '_score'].max()
        #     df_force[col + '_score'] = max_value * (1 - df_top_companies['rank'] / 100).values
        # data_indus.update(df_force)

    return data_indus


def zskxx_output_mode_score(data_indus, weight_path):
    """
    按照权重参数文件输出各个二级指标以及一级模块得分and招商可行性总分
    :param data_indus:指标得分结果表
    :return:
    """
    data_indus = copy.deepcopy(data_indus)

    with open(weight_path, 'r') as f:
        weight_dict = json.load(f)

    # 根据指标文件定义每个二级指标对应的指标得分名称
    # 产业市场空间
    sccykj_score_list = ['K1_score', 'K2_score', 'K3_score', 'K4_score',
                         'K5_score', 'K6_score', 'K9_score', 'K10_score']
    # 行业竞争
    hyjz_score_list = ['K12_score']

    # 区域布局
    qybj_score_list = ['K13_score']
    # 客户市场
    khsc_score_list = ['K15_score']

    # 盈利能力
    ylnl_score_list = ['K17_score', 'K18_score', 'K19_score',
                       'K20_score', 'K21_score']
    # 纳税能力
    nsnl_score_list = ['K22_score', 'K23_score']
    # 研发能力
    yfnl_score_list = ['K24_score', 'K25_score']
    # 团队能力
    tdnl_score_list = ['K26_score', 'K27_score', 'K28_score']

    # 投资偏好
    tzph_score_list = ['K33_score', 'K34_score']
    # 投资活跃度
    tzhyd_score_list = ['K35_score', 'K36_score', 'K37_score']

    # 计算二级指标得分（全部是列求和然后取均值）
    second_class_mode = [sccykj_score_list, hyjz_score_list,
                         qybj_score_list, khsc_score_list,
                         ylnl_score_list, nsnl_score_list, yfnl_score_list, tdnl_score_list,
                         tzph_score_list, tzhyd_score_list]

    second_class_name = ['sccykj_score', 'hyjz_score',
                         'qybj_score', 'khsc_score',
                         'ylnl_score', 'nsnl_score', 'yfnl_score', 'tdnl_score',
                         'tzph_score', 'tzhyd_score']

    for i in range(len(second_class_mode)):
        # print(i)
        # time1 = time.time()
        data_indus.loc[:, second_class_name[i]] = data_indus[second_class_mode[i]].mean(axis=1)
        # time2 = time.time()
        # print(time2 - time1)

    # Added by meijian on 20210302: 二级指标全部放缩到最大m分
    def max_min_trans(x):
        "二级指标最大最小归一化"
        if x.max() == x.min():
            return 50
        else:
            return (x - x.min()) / (x.max() - x.min()) * 99

    for col in second_class_name:
        data_indus[col] = max_min_trans(data_indus[col])

    # 计算模块得分
    hyqspg_score_model_content = ['sccykj_score', 'hyjz_score']
    ysxfx_score_model_content = ['qybj_score', 'khsc_score']
    nlxpg_score_model_content = ['ylnl_score', 'nsnl_score', 'yfnl_score', 'tdnl_score']
    qyxwx_score_model_content = ['tzph_score', 'tzhyd_score']
    first_class_mode_name = ['hyqspg_score', 'ysxfx_score', 'nlxpg_score', 'qyxwx_score']
    first_class_include_second_content = [hyqspg_score_model_content, ysxfx_score_model_content,
                                          nlxpg_score_model_content, qyxwx_score_model_content]

    # Added by meijian on 20200304: 计算各个一级模块得分以及总分，改写一下简化脚本
    for i in range(len(first_class_mode_name)):
        data_indus.loc[:, first_class_mode_name[i]] = 0
        for j in range(len(first_class_include_second_content[i])):
            data_indus.loc[:, first_class_mode_name[i]] += data_indus[first_class_include_second_content[i][j]] * \
                                                           weight_dict['second_class_weight'][first_class_mode_name[i]][
                                                               first_class_include_second_content[i][j]]

    # 计算招商可行性总分
    data_indus.loc[:, 'zskxx_score'] = 0
    for i in range(len(first_class_mode_name)):
        data_indus.loc[:, 'zskxx_score'] += data_indus[first_class_mode_name[i]] * \
                                            weight_dict['fisrt_class_weight']['zskxx'][first_class_mode_name[i]]

    # Added by meijian on 20210302: 对最后的招商可行性的总分再进行一次放缩
    # data_indus['zskxx_score'] = max_min_trans(data_indus['zskxx_score'])

    return data_indus


def zsjz_output_mode_score(data_indus, weight_path):
    """
    按照权重参数文件输出各个二级指标以及一级模块得分and招商价值总分
    :param data_indus:指标得分结果表
    :return:
    """
    data_indus = copy.deepcopy(data_indus)

    with open(weight_path, 'r') as f:
        weight_dict = json.load(f)

    # 根据指标文件定义每个二级指标对应的指标得分名称
    # 综合税收（前3年）
    zhss_3years_score_list = ['S1_score']
    # 综合税率
    zhsl_score_list = ['S2_score']

    # 企业获得资质的质量
    company_zzzl_score_list = ['S3_score']
    # 企业获得资质的数量
    company_zzsl_score_list = ['S4_score']

    # 企业的商标
    sb_num_score_list = ['S5_score']
    # 软件著作
    rz_num_score_list = ['S6_score']
    # 专利
    zl_score_list = ['S7_score', 'S8_score', 'S9_score']
    # 授牌
    sp_score_list = ['S10_score']

    # 高学历比例
    tzph_score_list = ['S19_score', 'S20_score']

    # 舆情总数
    yq_num_score_list = ['S28_score']
    # 荣誉类信息数据
    rylxx_score_list = ['S29_score']

    # 计算二级指标得分（全部是列求和然后取均值）
    second_class_mode = [zhss_3years_score_list, zhsl_score_list,
                         company_zzzl_score_list, company_zzsl_score_list,
                         sb_num_score_list, rz_num_score_list, zl_score_list, sp_score_list,
                         tzph_score_list,
                         yq_num_score_list, rylxx_score_list]

    second_class_name = ['zhss_3years_score', 'zhsl_score',
                         'company_zzzl_score', 'company_zzsl_score',
                         'sb_num_score', 'rz_num_score', 'zl_score', 'sp_score',
                         'gxlbl_score',
                         'yq_num_score', 'rylxx_score']

    for i in range(len(second_class_mode)):
        # print(i)
        # time1 = time.time()
        data_indus.loc[:, second_class_name[i]] = data_indus[second_class_mode[i]].mean(axis=1)
        # time2 = time.time()
        # print(time2-time1)

    def max_min_trans(x):
        "二级指标最大最小归一化"
        if x.max() == x.min():
            return 50
        else:
            return (x - x.min()) / (x.max() - x.min()) * 99

    for col in second_class_name:
        data_indus[col] = max_min_trans(data_indus[col])
    # 计算模块得分

    ns_ability_score_model_content = ['zhss_3years_score', 'zhsl_score']
    zz_score_model_content = ['company_zzzl_score', 'company_zzsl_score']
    create_ability_score_model_content = ['sb_num_score', 'rz_num_score', 'zl_score', 'sp_score']
    rcgx_score_model_content = ['gxlbl_score']
    yq_score_model_content = ['yq_num_score', 'rylxx_score']

    first_class_mode_name = ['ns_ability_score', 'zz_score', 'create_ability_score',
                             'rcgx_score', 'yq_score']
    first_class_include_second_content = [ns_ability_score_model_content, zz_score_model_content,
                                          create_ability_score_model_content, rcgx_score_model_content,
                                          yq_score_model_content]

    # Added by meijian on 20200304: 计算各个一级模块得分以及总分，改写一下简化脚本
    for i in range(len(first_class_mode_name)):
        data_indus.loc[:, first_class_mode_name[i]] = 0
        for j in range(len(first_class_include_second_content[i])):
            data_indus.loc[:, first_class_mode_name[i]] += data_indus[first_class_include_second_content[i][j]] * \
                                                           weight_dict['second_class_weight'][first_class_mode_name[i]][first_class_include_second_content[i][j]]

    # 计算招商价值总分
    data_indus.loc[:, 'zsjz_score'] = 0
    for i in range(len(first_class_mode_name)):
        data_indus.loc[:, 'zsjz_score'] += data_indus[first_class_mode_name[i]] * weight_dict['fisrt_class_weight']['zsjz'][first_class_mode_name[i]]

    return data_indus


def kjx_output_mode_score(data_indus, weight_path):
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
    first_class_mode_name = ['hxjsqk_score', 'yfnlqk_score', 'yfscrkqk_score',
                             'xdjzysqk_score', 'jscgzhqk_score']
    first_class_include_second_content = [hxjsqk_score_list, yfnlqk_score_list,
                                          yfscrkqk_score_list, xdjzysqk_score_list,
                                          jscgzhqk_score_list]

    # Added by meijian on 20200304: 计算各个一级模块得分以及总分，改写一下简化脚本
    for i in range(len(first_class_mode_name)):
        data_indus.loc[:, first_class_mode_name[i]] = 0
        for j in range(len(first_class_include_second_content[i])):
            data_indus.loc[:, first_class_mode_name[i]] += data_indus[first_class_include_second_content[i][j]] * \
                                                           all_index_weight_dict[first_class_include_second_content[i][j]]

    # 将一级模块的分数放缩到99
    def max_min_trans(x):
        "二级指标最大最小归一化"
        if x.max() == x.min():
            return 50
        else:
            return (x - x.min()) / (x.max() - x.min()) * 99

    for col in first_class_mode_name:
        data_indus[col] = max_min_trans(data_indus[col])

    # 计算招商价值总分
    data_indus.loc[:, 'kjx_score'] = 0
    for i in range(len(first_class_mode_name)):
        data_indus.loc[:, 'kjx_score'] += data_indus[first_class_mode_name[i]] * \
                                          weight_dict['fisrt_class_weight']['kjxczxqypf'][first_class_mode_name[i]]
    return data_indus


def output_result(result_xcl, result_xny, result_lcdl, output_path, index_symbol, mode_score_name):
    """
    :param result_xcl:
    :param result_xny:
    :param result_lcdl: 3个产业的结果表
    :param output_path: 结果输出路径
    :param index_symbol: 不同模块对应的指标编号首字母,'K'(招商可行性) OR 'S'（招商价值）
    :param mode_score_name: 不同模块输出的总分名称eg‘zskxx_score’
    :return:
    """

    def result_deal(data):
        # 选出需要的列
        data = copy.deepcopy(data)
        output_col = ['bbd_qyxx_id', 'industry'] + [_ for _ in data.columns if 'score' in _ and index_symbol not in _]
        data_ = data.loc[:, output_col]
        # 行业内部排序
        data__ = data_.sort_values(by=[mode_score_name], ascending=False)
        # Chen adds rank
        import numpy as np
        data__['rank'] = np.arange(len(data__)) + 1

        return data__

    result_xcl_ = result_deal(result_xcl)
    result_xny_ = result_deal(result_xny)
    result_lcdl_ = result_deal(result_lcdl)

    zskxx_result = pd.concat([result_xcl_,
                              result_xny_,
                              result_lcdl_])

    # 输出结果
    zskxx_result.to_csv(output_path, index=False, encoding='utf-8')
    return zskxx_result


def kjx_output_result(result_xcl, result_xny, result_lcdl, output_path):
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
                                 'xdjzysqk_score', 'jscgzhqk_score', 'kjx_score']
        output_col = ['bbd_qyxx_id', 'industry'] + first_class_mode_name
        data_ = data.loc[:, output_col]
        # 行业内部排序
        data__ = data_.sort_values(by=['kjx_score'], ascending=False)
        return data__

    result_xcl_ = result_deal(result_xcl)
    result_xny_ = result_deal(result_xny)
    result_lcdl_ = result_deal(result_lcdl)

    kjx_result = pd.concat([result_xcl_,
                            result_xny_,
                            result_lcdl_])

    # 输出结果
    kjx_result.to_csv(output_path, index=False, encoding='utf-8')
    return kjx_result


def result_normalized_trans(data, col):
    """
    :param data: 分行业需要正态化的数据
    :param col: 正态化变化列名
    :return: 正太化后的结果
    """
    import numpy as np
    # 获取分位点
    seven_point = np.percentile(data[col], 70, interpolation='linear')
    nine_point = np.percentile(data[col], 90, interpolation='linear')

    # 切分数据
    data_less_seven_point = data.loc[data[col] <= seven_point, :]
    data_middle_point = data.loc[data[col] > seven_point, :]
    data_middle_point = data_middle_point.loc[data_middle_point[col] < nine_point, :]
    data_more_nine_point = data.loc[data[col] >= nine_point, :]

    # 分数转换（原来的最大最小[a,b];线性拉升后的最大最小[c,d]）
    def linear_trans(x, c, d):
        """
        :param x: a,b,c,d原数据以及拉升后的最小最大值
                  0.7分位点以下【x.min(),seven_point】-->>[7,60];
                  0.7-0.9分位点【seven_point，nine_point】-->>[60,80]
                  0.9分位点以上【nine_point,x.max()】-->>【80,99】
        :return:
        """
        a = x.min()
        b = x.max()
        if a == b:
            return c
        else:
            k = (d - c) / (b - a)
            j = c - a * k
            return k * x + j

    data_less_seven_point[col] = linear_trans(data_less_seven_point[col], 40, 60)
    data_middle_point[col] = linear_trans(data_middle_point[col], 60, 80)
    data_more_nine_point[col] = linear_trans(data_more_nine_point[col], 80, 99)

    # 由于存在某些模块的最大最小都为0，结果为空值，需要进行填充0值
    data_less_seven_point[col] = data_less_seven_point[col].fillna(0)

    # 合并结果
    data_trans = pd.concat([data_less_seven_point,
                            data_middle_point,
                            data_more_nine_point])

    return data_trans


def trans_all(data, trans_col, df_top_companies):
    """
    对结果数据的得分和模块得分均进行转化
    :param data:
    :return:
    """
    companies = df_top_companies['bbd_qyxx_id'].tolist()
    df_force = data[data['bbd_qyxx_id'].isin(companies)]
    for col in trans_col:
        data = result_normalized_trans(data, col)
        data = data.drop_duplicates('bbd_qyxx_id')
        # 定向加分 Chen added
        max_value = data[col].max()
        df_force[col] = max_value * (1 - df_top_companies['rank'] / 1000).values
    data.update(df_force)
    return data

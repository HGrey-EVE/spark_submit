# -*- coding: utf-8 -*-

"""
Created On Mon 1 March 2021

@author Nathan

Email: chenchen@bbdservice.com

Instruction: 重庆现代产业研究院
"""

import os
import time
import json
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")


def func_timer(func):
    def func_wrapper(*args, **kwargs):
        time_start = time.clock()
        result = func(*args, **kwargs)
        time_end = time.clock()
        time_taken = time_end - time_start
        print(f'{func.__name__} running time: {round(time_taken, 2)} s')
        return result

    return func_wrapper


class ReadData(object):
    def __init__(self, path, data_file, sep, weight_file):
        self.path = path
        self.data_file = data_file
        self.separator = sep
        self.data = None
        self.weight_file = weight_file
        self.weight = None

    @func_timer
    def read_data_csv(self):
        self.data = pd.read_csv(os.path.join(self.path, self.data_file), sep='\t')
        # remove repeating headers in the file merged from c6
        header_index = self.data[self.data['bbd_qyxx_id'] == 'bbd_qyxx_id'].index.tolist()
        self.data.drop(header_index, inplace=True)

        return self.data

    @func_timer
    def read_top_csv(self, file_top_company):
        data_top_companies = pd.read_csv(file_top_company, sep=',')

        return data_top_companies

    @func_timer
    def read_weight(self):
        with open(os.path.join(self.path, self.weight_file), 'r') as f:
            weight_dict = json.load(f)

            return weight_dict


class MergeCsvHadoop(object):
    def __init__(self):
        self.path = None
        self.file_type = None
        self.file_output = None

    def merge_data(self, path, file_type, file_output):
        self.path = path
        self.file_type = file_type
        self.file_output = file_output
        cmd = f"hadoop fs -getmerge {self.path}/*.{self.file_type} {self.file_output}"
        os.system(cmd)


class Normalize(object):
    @staticmethod
    def normalize_zscore_field(df, field):
        if df[field].max() == df[field].min():
            values = df[[field]].apply(lambda x: x)
        else:
            values = df[[field]].apply(lambda x: (x - x.mean()) / x.std())

        return values

    @staticmethod
    def normalize_maxmin_field(df, field):
        # def normalize(x):
        #     if x.max() == x.min():
        #         return x
        #     else:
        #         return (x - x.min()) / (x.max() - x.min()) * max_score
        # values = df[field].apply(lambda x: normalize(x))
        if df[field].max() == df[field].min():
            values = df[[field]].apply(lambda x: x)
        else:
            values = df[[field]].apply(lambda x: (x - x.min()) / (x.max() - x.min()))

        return values


class Evaluation(object):
    def __init__(self, df_score, df_weight):
        self.df_score = df_score
        self.weight = df_weight

    def __repr__(self):
        return f"An instance to evaluate score based on weight."

    @func_timer
    def second_class(self):
        second_class_weight = self.weight.index.values
        for weight_name in second_class_weight:
            weight = self.weight.loc[weight_name].value_counts().index.values
            self.df_score[f'{weight_name}_score'] *= weight

    @func_timer
    def first_class(self):
        first_class_weight = self.weight.columns
        for weight_name in first_class_weight:
            second_class_labels = self.weight[self.weight[weight_name].notnull()][f'{weight_name}'].index.tolist()
            second_class_labels = [f'{label}_score' for label in second_class_labels]
            self.df_score[f'{weight_name}'] = self.df_score[second_class_labels].sum(axis=1)

    @func_timer
    def total_score(self):
        first_class_labels = self.weight.columns
        self.df_score['total'] = self.df_score[first_class_labels].sum(axis=1)

    @func_timer
    def result_normalized_trans(self, data, col):
        """
        :param data: 分行业需要正态化的数据
        :param col: 正态化变化列名
        :return: 正太化后的结果
        """

        # Mei Jian
        # # 获取分位点
        # seven_point = np.percentile(data[col], 70, interpolation='linear')
        # nine_point = np.percentile(data[col], 90, interpolation='linear')
        # # 切分数据
        # data_less_seven_point = data.loc[data[col] <= seven_point, :]
        # data_middle_point = data.loc[data[col] > seven_point, :]
        # data_middle_point = data_middle_point.loc[data_middle_point[col] < nine_point, :]
        # data_more_nine_point = data.loc[data[col] >= nine_point, :]

        # Chen Chen
        # 获取分位点
        percentile_70 = np.percentile(data[col], 70, interpolation='linear')
        percentile_90 = np.percentile(data[col], 90, interpolation='linear')

        # 切分数据
        data_lt_70 = data.loc[data[col] <= percentile_70, :]
        data_in_between = data.loc[data[col] > percentile_70, :]
        data_in_between = data_in_between.loc[data_in_between[col] < percentile_90, :]
        data_gt_90 = data.loc[data[col] >= percentile_90, :]

        # 分数转换（原来的最大最小[a,b]; 线性拉升后的最大最小[c,d]）
        def linear_trans(x, c, d):
            """
            :param x: a, b, c, d原数据以及拉升后的最小最大值
                      0.7分位点以下【x.min(), percentile_70】-->>[7,60];
                      0.7-0.9分位点【percentile_70, data_gt_90】-->>[60,80]
                      0.9分位点以上【data_gt_90, x.max()】-->>【80,99】
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

        data_lt_70[col] = linear_trans(data_lt_70[col], 7, 60)
        data_in_between[col] = linear_trans(data_in_between[col], 60, 80)
        data_gt_90[col] = linear_trans(data_gt_90[col], 80, 99)

        # 由于存在某些模块的最大最小都为0，结果为空值，需要进行填充0值
        data_lt_70[col] = data_lt_70[col].fillna(0)

        # 合并结果
        data_trans = pd.concat([data_lt_70, data_in_between, data_gt_90])

        return data_trans

    def trans_all(self, data, trans_col):
        """
        对结果数据的得分和模块得分均进行转化
        :param data:
        :return:
        """
        for col in trans_col:
            data = self.result_normalized_trans(data, col)
            data = data.drop_duplicates('bbd_qyxx_id')
        return data

    def result_adjust(self, n=6):
        """n表示需要调整的模块分数跟总分的个数，从倒数第n列开始调整
           最后6列：['profit_capability', 'operate_capability', 'industry_status', 'related_power', 'technology_capability', 'total']
        """
        print('score adjust...')
        # by Mei Jian
        # data_risk_xcl = self.df_score.loc[self.df_score['industry'] == '新材料', :]
        # data_risk_xny = self.df_score.loc[self.df_score['industry'] == '新能源', :]
        # data_risk_jcdl = self.df_score.loc[self.df_score['industry'] == '集成电路', :]
        # trans_col = list(self.df_score.columns)[-n:]
        # data_xcl_result_trans = self.trans_all(data_risk_xcl, trans_col)
        # data_xny_result_trans = self.trans_all(data_risk_xny, trans_col)
        # data_jcdl_result_trans = self.trans_all(data_risk_jcdl, trans_col)
        # self.df_score = pd.concat([data_xcl_result_trans, data_xny_result_trans, data_jcdl_result_trans])

        # Modified by Chen
        sub_industries = self.df_score['sub_industry'].unique()
        trans_col = list(self.df_score.columns)[-n:]
        for sub_industry in sub_industries:
            print(f'adjusting sub_industry score: {sub_industry}')
            data_sub_industry_result = self.df_score.loc[self.df_score['sub_industry'] == sub_industry, :]
            data_sub_industry_result_trans = self.trans_all(data_sub_industry_result, trans_col)
            self.df_score.update(data_sub_industry_result_trans)

    # Added by Chen
    def compare(self, df_target):
        """对比产品提供的行业龙头公司在模型评分中的排名"""
        sub_industries = self.df_score['sub_industry'].unique()
        self.df_score['rank'] = np.nan
        for sub_industry in sub_industries:
            print(f'ranking sub_industry: {sub_industry} {list(sub_industries).index(sub_industry)}/{len(sub_industries)}')
            df_score_subindustry = self.df_score[self.df_score['sub_industry'] == sub_industry].sort_values(by='total', ascending=False)
            df_score_subindustry['rank'] = np.arange(len(df_score_subindustry)) + 1
            self.df_score.update(df_score_subindustry)
        df_merge = pd.merge(df_target, self.df_score, how='left', on='bbd_qyxx_id', suffixes=['', '_model'])

        return df_merge

    @func_timer
    def output(self, path, file):
        print('Output...')
        self.df_score.to_csv(os.path.join(path, file), encoding='utf-8', index=False)


# -*- coding: utf-8 -*-

"""
Created On Mon 3 March 2021

@author Nathan

Email: chenchen@bbdservice.com

Instruction: 重庆现代产业研究院
"""
import sys

from helper import *
import numpy as np
import warnings
warnings.filterwarnings("ignore")

PATH_FILE = os.path.dirname(__file__)


class ProcessData(Normalize):
    def __init__(self, df):
        self.df = df

    def __repr__(self):
        return "归一化处理"

    @func_timer
    def scoring_rank(self, field, max_score):
        values = Normalize.normalize_maxmin_field(self.df, field) * max_score

        return values

    def force_score(self, field, df_top_companies):
        """根据龙头企业信息加分，取出子行业（self.df）各指标列（field）的最大值，根据龙头行业给定的rank赋值"""
        field_max_value = self.df[field].max()
        companies = df_top_companies['bbd_qyxx_id'].tolist()
        df_force = self.df[self.df['bbd_qyxx_id'].isin(companies)]
        df_force[field] = field_max_value * (1 - df_top_companies['rank'] / 100).values
        # 中芯国际上调
        df_zxgj = df_top_companies[df_top_companies['company_name'].str.contains('中芯国际集成电路制造（上海）')]
        if not df_zxgj.empty:
            qyxx_zxgj = df_zxgj['bbd_qyxx_id'].iloc[0]
            df_force.loc[df_force['bbd_qyxx_id'] == qyxx_zxgj, field] = field_max_value * 0.999
        # 长电科技上调
        df_cdkj = df_top_companies[df_top_companies['company_name'].str.contains('长电科技')]
        if not df_cdkj.empty:
            qyxx_cdkj = df_cdkj['bbd_qyxx_id'].iloc[0]
            df_force.loc[df_force['bbd_qyxx_id'] == qyxx_cdkj, field] = field_max_value * 0.995
        # 英特尔（成都）下调
        df_intelcd = df_top_companies[df_top_companies['company_name'].str.contains('英特尔产品（成都）')]
        if not df_intelcd.empty:
            qyxx_intelcd = df_intelcd['bbd_qyxx_id'].iloc[0]
            df_force.loc[df_force['bbd_qyxx_id'] == qyxx_intelcd, field] = field_max_value * (1 - df_top_companies['rank'].max() / 100)

        self.df.update(df_force)

        pass


def main():
    dt = sys.argv[1]
    # 合并集群数据
    merge = MergeCsvHadoop()
    merge.merge_data(f'/user/cqxdcyfzyjy/{dt}/tmp/cqxdcy/cq_xdcy_tzjz_invest', 'csv', 'invest.csv')

    # 读取数据
    file_name = 'invest.csv'  #corrected by meijian on 20210323
    # file_name = 'invest_0609.csv'
    weight_name = 'weight_invest.json'
    separator = '\t'
    data_reader = ReadData(path=PATH_FILE, data_file=file_name, sep=separator, weight_file=weight_name)
    # 读取数据
    df_raw = data_reader.read_data_csv()
    # 有重复
    df_raw = df_raw.drop_duplicates('bbd_qyxx_id')
    # 替换正负inf为NA，加inplace参数, T15/T16
    df_raw = df_raw.replace([np.inf, -np.inf], np.nan)
    # 空值填0
    df_raw = df_raw.fillna(0)
    # df_raw.drop('T39', axis=1, inplace=True)
    # T39 发明专利数量数据有误
    df_raw.T39 = df_raw.T39.apply(lambda x: 0)
    # T42与T45一样，T49与T52一样，T39与T60一样
    df_raw['T45'] = df_raw['T42']
    df_raw['T52'] = df_raw['T49']
    df_raw['T60'] = df_raw['T39']
    # 计算T43/T44
    df_raw['T43'] = df_raw[
        ['T_18Y', 'T_19Y', 'T_20Y', 'T_21Y', 'T_22Y', 'T_23Y', 'T_24Y', 'T_25Y', 'T_26Y', 'T_27Y', 'T_28Y', 'T_29Y',
         'T_30Y', 'T_31Y']].sum(axis=1)
    df_raw['T44'] = df_raw[['T_18Y', 'T_19Y', 'T_20Y', 'T_22Y', 'T_29Y', 'T_30Y']].sum(axis=1)
    df_raw.drop(
        ['T_18Y', 'T_19Y', 'T_20Y', 'T_21Y', 'T_22Y', 'T_23Y', 'T_24Y', 'T_25Y', 'T_26Y', 'T_27Y', 'T_28Y', 'T_29Y',
         'T_30Y', 'T_31Y'], axis=1, inplace=True)

    # 读取龙头公司数
    data_top_companies = data_reader.read_top_csv(os.path.join('.', 'top_companies.csv'))

    sub_industries = df_raw.sub_industry.unique()
    columns = df_raw.columns
    company_info = ['bbd_qyxx_id', 'industry', 'sub_industry', 'province', 'city', 'region']
    # 指标列
    index_columns = [column for column in columns if column not in company_info]
    # 指标评分列名
    score_columns = [f"{column}_score" if column not in company_info else column for column in columns]
    # 创建评分df
    df_score = pd.DataFrame(columns=score_columns)
    # 填充公司信息
    df_score[company_info] = df_raw[company_info]

    # 处理数据
    for sub_industry in sub_industries:
        df_raw_sub_industry = df_raw[df_raw.sub_industry == sub_industry].copy()
        df_score_sub_industry = df_score[df_score.sub_industry == sub_industry].copy()
        proc = ProcessData(df=df_raw_sub_industry)
        for column in index_columns:
            print(f'{sub_industry}@{column}')
            # 根据龙头公司表定向加分
            proc.force_score(field=column,
                             df_top_companies=data_top_companies[data_top_companies['sub_industry'] == sub_industry])
            scores = proc.scoring_rank(field=column, max_score=99)
            df_score_sub_industry[f'{column}_score'] = scores
        df_score.update(df_score_sub_industry)

    # 读取权重
    dict_weight = data_reader.read_weight()
    df_weight = pd.DataFrame(dict_weight)
    # 算总分
    evl = Evaluation(df_score=df_score, df_weight=df_weight)
    evl.second_class()
    evl.first_class()
    evl.total_score()
    evl.result_adjust(6)
    # # 对比龙头企业
    data_top_companies = pd.read_csv(os.path.join('.', 'top_companies.csv'), sep=',')
    df_compare = evl.compare(df_target=data_top_companies)
    evl.output(PATH_FILE, 'invest_scores.csv')

    print("程序结束。")


if __name__ == '__main__':
    main()

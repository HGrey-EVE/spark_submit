# -*- coding: utf-8 -*-

"""
Created On Mon 2 March 2021

@author Nathan

Email: chenchen@bbdservice.com

Instruction: 重庆现代产业研究院
"""
import sys

from helper import *
import warnings
warnings.filterwarnings("ignore")

PATH_FILE = os.path.dirname(__file__)

class ProcessData(object):
    def __init__(self, df):
        self.df = df

    @func_timer
    def scoring_quantile(self, field, score_max=10):
        self.df[field] = self.df[field].astype('float')
        qtl1 = self.df[field].quantile(0.25)
        qtl2 = self.df[field].quantile(0.5)
        qtl3 = self.df[field].quantile(0.75)

        def score_level(value, max=score_max):
            if value == 0:
                s = 0
            elif 0 < value <= qtl1:
                s = max * 0.25
            elif qtl1 < value <= qtl2:
                s = max * 0.5
            elif qtl2 < value <= qtl3:
                s = max * 0.75
            else:
                s = max
            return s
        _values = self.df[field].apply(lambda x: score_level(x, score_max))

        return _values

    @func_timer
    def scoring_yes_no(self, field, threshold, yes_score, no_score, flag=0):
        global _values
        self.df[field] = self.df[field].astype('float')
        if flag == 0:
            def score_level(value):
                if value >= threshold:
                    s = yes_score
                else:
                    s = no_score
                return s
            _values = self.df[field].apply(lambda x: score_level(x))

        elif flag == 1:
            def score_level(value):
                if value != threshold:
                    s = yes_score
                else:
                    s = no_score
                return s
            _values = self.df[field].apply(lambda x: score_level(x))
        else:
            raise Exception("Wrong flag input, please set flag correctly.")
        return _values


def main():
    version = sys.argv[1]

    # 合并集群数据
    merge = MergeCsvHadoop()
    merge.merge_data(f'/user/cqxdcyfzyjy/{version}/tmp/cqxdcy/cq_xdcy_invest_risk_index', 'csv', 'risk.csv')

    # 数据文件路径
    file_name = 'risk.csv'  #corrected by meijian on 20210323
    weight_name = 'weight_risk.json'
    separator = '\t'
    data_reader = ReadData(path=PATH_FILE, data_file=file_name, sep=separator, weight_file=weight_name)
    # 读取数据
    df_raw = data_reader.read_data_csv()

    columns = df_raw.columns
    company_info = ['bbd_qyxx_id', 'company_name', 'province', 'city', 'district', 'industry', 'sub_industry']
    # 指标列
    index_columns = [column for column in columns if column not in company_info]
    # 指标评分列名
    score_columns = [f"{column}_score" if column not in company_info else column for column in columns]
    # 创建评分df
    df_score = pd.DataFrame(columns=score_columns)
    # 填充公司信息
    df_score[company_info] = df_raw[company_info]
    # “是否”指标
    index_columns_yesno = ['1Y', '382C', '383C', '489C', '449C', '161C', '294C', '295C', '17Y']
    # quantile指标
    index_columns_quantiles = [column for column in index_columns if column not in index_columns_yesno]

    proc = ProcessData(df=df_raw)
    # quantile记分
    for column in index_columns_quantiles:
        print(f'{column}')
        scores = proc.scoring_quantile(field=column)
        df_score[f'{column}_score'] = scores
        # ProcessData.normalize_maxmin_field(df_score, f'{column}_score', max_score=10)

    # yesno记分
    for column in index_columns_yesno:
        print(f'{column}')
        if column == '1Y':
            scores = proc.scoring_yes_no(field=column, threshold=1E5, yes_score=4, no_score=0)
        elif column == '382C':
            scores = proc.scoring_yes_no(field=column, threshold=0, yes_score=4, no_score=0, flag=1)
        elif column == '383C':
            scores = proc.scoring_yes_no(field=column, threshold=3, yes_score=4, no_score=0)
        else:
            scores = proc.scoring_yes_no(field=column, threshold=0, yes_score=0, no_score=4, flag=1)
        df_score[f'{column}_score'] = scores

    # 读取权重
    dict_weight = data_reader.read_weight()
    df_weight = pd.DataFrame(dict_weight)
    # 算总分
    evl = Evaluation(df_score=df_score, df_weight=df_weight)
    evl.second_class()
    evl.first_class()
    evl.total_score()
    evl.result_adjust(4)
    evl.output(PATH_FILE, 'risk_result.csv')


if __name__ == '__main__':
    main()






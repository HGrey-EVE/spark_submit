#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created On Tue 9 Mar 2021

@author john

Email: meijian@bbdservice.com

Instruction: 重庆产业链项目模型315上线主函数
"""
import sys

import BaseUtils
from helper import *
import warnings

warnings.filterwarnings("ignore")


class cqcyl_main_model():
    """招商可行性模块"""

    def __init__(self, data_path, weight_path, output_path, discrete_index, negative_index):
        # 数据路径
        self.data_path = data_path
        # 权重表路径
        self.weight_path = weight_path
        # 结果输出路径
        self.output_path = output_path
        # 离散型指标列表
        self.discrete_index = discrete_index
        # 负向指标列表
        self.negative_index = negative_index
        # 读取龙头公司数
        self.data_top_companies = pd.read_csv('top_companies.csv')
        self.basic_headers = ['bbd_qyxx_id', 'industry', 'sub_industry', 'rank']

    "数据获取、清洗"

    def data_process(self, model_name_sybol):

        global trans_col, index_symbol, mode_score_name, output_col
        print("step1:获取数据")
        # self.data__xcl, self.data__xny, self.data__infotech = BaseUtils.read_data(self.data_path)
        self.data = BaseUtils.read_data(self.data_path)
        tmp = pd.DataFrame()
        if model_name_sybol == 'zskxx':
            trans_col = ['zskxx_score', 'hyqspg_score', 'ysxfx_score', 'nlxpg_score', 'qyxwx_score']
            index_symbol = 'K'
            mode_score_name = 'zskxx_score'
            output_col = self.basic_headers + [_ for _ in self.data.columns if 'score' in _ and index_symbol not in _]
        if model_name_sybol == 'zsjz':
            trans_col = ['zsjz_score', 'ns_ability_score', 'zz_score', 'create_ability_score', 'rcgx_score', 'yq_score']
            index_symbol = 'S'
            mode_score_name = 'zsjz_score'
            output_col = self.basic_headers + [_ for _ in self.data.columns if 'score' in _ and index_symbol not in _]
        if model_name_sybol == 'kjx':
            mode_score_name = 'kjx_score'
            trans_col = ['kjx_score', 'hxjsqk_score', 'yfnlqk_score', 'yfscrkqk_score',
                         'xdjzysqk_score', 'jscgzhqk_score']
            # first_class_mode_name = ['hxjsqk_score', 'yfnlqk_score', 'yfscrkqk_score',
            #                          'xdjzysqk_score', 'jscgzhqk_score', 'kjx_score']
            output_col = ['bbd_qyxx_id', 'industry'] + trans_col


        # print("step2:数据处理")
        # self.data__xcl_deal = BaseUtils.data_deal(self.data__xcl, self.discrete_index)
        # self.data__xny_deal = BaseUtils.data_deal(self.data__xny, self.discrete_index)
        # self.data__infotech_deal = BaseUtils.data_deal(self.data__infotech, self.discrete_index)
        sub_industries = self.data['sub_industry'].unique()
        for sub_industry in sub_industries[:]:
            print(f"step2:{model_name_sybol}数据处理 {sub_industry}, {list(sub_industries).index(sub_industry)}/{len(sub_industries)}")
            self.data_subindustry = self.data[self.data['sub_industry'] == sub_industry]
            self.data_subindustry_deal = BaseUtils.data_deal(self.data_subindustry, self.discrete_index)

            print(f"step3:{model_name_sybol}指标得分计算 {sub_industry}, {list(sub_industries).index(sub_industry)}/{len(sub_industries)}")
            self._subindustry_index_score = BaseUtils.single_index_score(self.data_subindustry_deal,
                                                                 self.discrete_index,
                                                                 self.negative_index)
        # top_company_xny = self.data_top_companies[self.data_top_companies['industry'] == '新能源']
        # self._xny_index_score = BaseUtils.single_index_score(self.data__xny_deal,
        #                                                      self.discrete_index,
        #                                                      self.negative_index, top_company_xny)
        # top_company_infotech = self.data_top_companies[self.data_top_companies['industry'] == '新一代信息技术']
        # self._infotech_index_score = BaseUtils.single_index_score(self.data__infotech_deal,
        #                                                       self.discrete_index,
        #                                                       self.negative_index, top_company_infotech)
        # if model_name_sybol == 'zskxx':
            print(f"step4:{model_name_sybol}模块得分计算 {sub_industry}, {list(sub_industries).index(sub_industry)}/{len(sub_industries)}")
            if model_name_sybol == 'zskxx':
                self._subindustry_result = BaseUtils.zskxx_output_mode_score(self._subindustry_index_score, self.weight_path)
            if model_name_sybol == 'zsjz':
                self._subindustry_result = BaseUtils.zsjz_output_mode_score(self._subindustry_index_score, self.weight_path)
            if model_name_sybol == 'kjx':
                self._subindustry_result = BaseUtils.kjx_output_mode_score(self._subindustry_index_score, self.weight_path)

            # self.zskxx_xcl_result = BaseUtils.zskxx_output_mode_score(self._xcl_index_score, self.weight_path)
            # self.zskxx_xny_result = BaseUtils.zskxx_output_mode_score(self._xny_index_score, self.weight_path)
            # self.zskxx_infotech_result = BaseUtils.zskxx_output_mode_score(self._infotech_index_score, self.weight_path)

            print(f"step5:{model_name_sybol}得分调整 {sub_industry}, {list(sub_industries).index(sub_industry)}/{len(sub_industries)}")
            top_company_subindustry = self.data_top_companies[self.data_top_companies['sub_industry'] == sub_industry]
            self._subindustry_result_trans = BaseUtils.trans_all(self._subindustry_result, trans_col, top_company_subindustry)
            # trans_col = ['zskxx_score', 'hyqspg_score', 'ysxfx_score', 'nlxpg_score', 'qyxwx_score']
            # self.zskxx_xcl_result_trans = BaseUtils.trans_all(self.zskxx_xcl_result, trans_col)
            # self.zskxx_xny_result_trans = BaseUtils.trans_all(self.zskxx_xny_result, trans_col)
            # self.zskxx_infotech_result_trans = BaseUtils.trans_all(self.zskxx_infotech_result, trans_col)

            # 行业内部排序
            self._subindustry_result_trans = self._subindustry_result_trans.sort_values(by=[mode_score_name], ascending=False)
            self._subindustry_result_trans['rank'] = np.arange(len(self._subindustry_result_trans)) + 1

            tmp = pd.concat([tmp, self._subindustry_result_trans])

        print("step6:输出结果")

        if model_name_sybol == 'zskxx':
            output_col = self.basic_headers + [_ for _ in tmp if 'score' in _ and index_symbol not in _]
        if model_name_sybol == 'zsjz':
            output_col = self.basic_headers + [_ for _ in tmp if 'score' in _ and index_symbol not in _]
        if model_name_sybol == 'kjx':
            output_col = ['bbd_qyxx_id', 'industry', 'sub_industry'] + trans_col

        tmp = tmp.loc[:, output_col]
        tmp.to_csv(self.output_path, index=False, encoding='utf-8')
        # index_symbol = 'K'
        # mode_score_name = 'zskxx_score'
        # self.zskxx_result = BaseUtils.output_result(self.zskxx_xcl_result_trans,
        #                                             self.zskxx_xny_result_trans,
        #                                             self.zskxx_infotech_result_trans,
        #                                             self.output_path,
        #                                             index_symbol,
        #                                             mode_score_name)
        print(f'结束{model_name_sybol}任务')
        # return self.zskxx_result

        # if model_name_sybol == 'zsjz':
        #     print("step4:模块得分计算")
        #     self.zsjz_xcl_result = BaseUtils.zsjz_output_mode_score(self._xcl_index_score, self.weight_path)
        #     self.zsjz_xny_result = BaseUtils.zsjz_output_mode_score(self._xny_index_score, self.weight_path)
        #     self.zsjz_infotech_result = BaseUtils.zsjz_output_mode_score(self._infotech_index_score, self.weight_path)
        #
        #     print('step5:得分调整')
        #     trans_col = ['zsjz_score', 'ns_ability_score', 'zz_score', 'create_ability_score', 'rcgx_score', 'yq_score']
        #     self.zsjz_xcl_result_trans = BaseUtils.trans_all(self.zsjz_xcl_result, trans_col)
        #     self.zsjz_xny_result_trans = BaseUtils.trans_all(self.zsjz_xny_result, trans_col)
        #     self.zsjz_infotech_result_trans = BaseUtils.trans_all(self.zsjz_infotech_result, trans_col)
        #
        #     print("step6:输出结果")
        #     index_symbol = 'S'
        #     mode_score_name = 'zsjz_score'
        #     self.zsjz_result = BaseUtils.output_result(self.zsjz_xcl_result_trans,
        #                                                self.zsjz_xny_result_trans,
        #                                                self.zsjz_infotech_result_trans,
        #                                                self.output_path,
        #                                                index_symbol,
        #                                                mode_score_name)
        #     print('结束招商价值任务')
        #     return self.zsjz_result

        # if model_name_sybol == 'kjx':
        #     print("step4:模块得分计算")
        #     self.kjx_xcl_result = BaseUtils.kjx_output_mode_score(self._xcl_index_score, self.weight_path)
        #     self.kjx_xny_result = BaseUtils.kjx_output_mode_score(self._xny_index_score, self.weight_path)
        #     self.kjx_infotech_result = BaseUtils.kjx_output_mode_score(self._infotech_index_score, self.weight_path)
        #
        #     print('step5:得分调整')
        #     trans_col = ['kjx_score', 'hxjsqk_score', 'yfnlqk_score', 'yfscrkqk_score',
        #                  'xdjzysqk_score', 'jscgzhqk_score']
        #     self.kjx_xcl_result_trans = BaseUtils.trans_all(self.kjx_xcl_result, trans_col)
        #     self.kjx_xny_result_trans = BaseUtils.trans_all(self.kjx_xny_result, trans_col)
        #     self.kjx_infotech_result_trans = BaseUtils.trans_all(self.kjx_infotech_result, trans_col)
        #
        #     print("step6:输出结果")
        #     self.kjx_result = BaseUtils.kjx_output_result(self.kjx_xcl_result_trans,
        #                                                   self.kjx_xny_result_trans,
        #                                                   self.kjx_infotech_result_trans,
        #                                                   self.output_path)
        #     print('结束科技型任务')
        #     return self.kjx_result


if __name__ == "__main__":
    dt = sys.argv[1]
    # 合并集群数据
    merge = MergeCsvHadoop()
    # merge.merge_data(f'/user/cqxdcyfzyjy/{dt}/tmp/cqxdcy/cq_xdcy_merchant/cq_xdcy_merchant_zskxx', 'csv', 'zskxx.csv')
    # merge.merge_data(f'/user/cqxdcyfzyjy/{dt}/tmp/cqxdcy/cq_xdcy_merchant/cq_xdcy_merchant_zsjz', 'csv', 'zsjz.csv')
    merge.merge_data(f'/user/cqxdcyfzyjy/{dt}/tmp/cqxdcy/cq_xdcy_kejixing/merge_index_kjx', 'csv', 'kjx.csv')

    # 本地测试是文件路径为'../zskxx.csv'，上线是改为'./zskxx.csv'
    weight_path = './weight.json'
    # zskxx_path = './zskxx_0609.csv'
    zskxx_path = './zskxx.csv'
    zskxx_output_path = './zskxx_result.csv'
    zskxx_discrete_index = ['K22']
    zskxx_negative_index = ['K23']

    # zsjz_path = './zsjz_0609.csv'
    zsjz_path = './zsjz.csv'
    zsjz_output_path = './zsjz_result.csv'
    zsjz_discrete_index = ['S1']
    zsjz_negative_index = ['S2']

    kjx_path = './kjx.csv'
    kjx_output_path = './kjx_result.csv'
    kjx_discrete_index = []
    kjx_negative_index = []

    # zskxx_model = cqcyl_main_model(zskxx_path, weight_path, zskxx_output_path, zskxx_discrete_index,
    #                                zskxx_negative_index)
    # zskxx_result = zskxx_model.data_process(model_name_sybol='zskxx')
    #
    # zsjz_model = cqcyl_main_model(zsjz_path, weight_path, zsjz_output_path, zsjz_discrete_index, zsjz_negative_index)
    # zsjz_result = zsjz_model.data_process(model_name_sybol='zsjz')

    kjx_model = cqcyl_main_model(kjx_path, weight_path, kjx_output_path, kjx_discrete_index, kjx_negative_index)
    kjx_result = kjx_model.data_process(model_name_sybol='kjx')

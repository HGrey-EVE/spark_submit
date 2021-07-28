#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: process_test
:Author: xufeng@bbdservice.com 
:Date: 2021-06-07 5:38 PM
:Version: v.1.0
:Description:
    graph table process test
"""
from pyspark.sql import DataFrame

from whetstone.core.entry import Entry
from scwgj.graph_subject_data.extract_core import GraphDataExtractTemplate, TableMetaData


class SubjectDataExtractTest(GraphDataExtractTemplate):

    TABLE_NAME = 'subject_test'
    TABLE_COLUMNS = ['bbd_qyxx_id', 'company_name', 'company_province', 'company_county']

    @staticmethod
    def _get_data_v202104():
        return [
            {
                'bbd_qyxx_id': 'ae906a1255cf4222805f2ca237d56c22', 'company_name': '广州市黄埔区智立科技开发部',
                'company_province': '广东', 'company_county': '440112'
            },
            {
                'bbd_qyxx_id': 'af11d10252404ce39d4402881fb00aaf', 'company_name': '北京四季建慧食品店',
                'company_province': '北京', 'company_county': '110102'
            },
            {
                'bbd_qyxx_id': 'af789ac00fca4bd8a2daecf1fe3fa207', 'company_name': '北京市玉泉营瑞丰利装饰材料经营部',
                'company_province': '北京', 'company_county': '110106'
            },
            {
                'bbd_qyxx_id': 'ae6a0f7bd8b041999a16358514e48e10', 'company_name': '北京环京盛业电子产品中心',
                'company_province': '北京', 'company_county': '110106'
            },
            {
                'bbd_qyxx_id': 'ae6b8d41c0834bb9a1d2a789e70b92ff', 'company_name': '北京域奇爱玛工艺品销售中心',
                'company_province': '北京', 'company_county': '110112'
            },
        ]

    @staticmethod
    def _get_data_v202105():
        return [
            {
                'bbd_qyxx_id': 'ae906a1255cf4222805f2ca237d56c22', 'company_name': '广州市黄埔区智立科技开发部',
                'company_province': '广东', 'company_county': '440112'
            },
            {
                'bbd_qyxx_id': 'af11d10252404ce39d4402881fb00aaf', 'company_name': '北京四季建慧食品店',
                'company_province': '北京', 'company_county': '110102'
            },
            {
                'bbd_qyxx_id': 'af789ac00fca4bd8a2daecf1fe3fa207', 'company_name': '北京市玉泉营瑞丰利装饰材料经营部',
                'company_province': '北京', 'company_county': '110106'
            },
            {
                'bbd_qyxx_id': 'ae6a0f7bd8b041999a16358514e48e10', 'company_name': '北京环京盛业电子产品中心',
                'company_province': '北京', 'company_county': '110107'
            },
            {
                'bbd_qyxx_id': 'b66777b2984147fa9d4736092c71c82b', 'company_name': '中国农业银行股份有限公司盘锦辽河支行',
                'company_province': '辽宁', 'company_county': '211103'
            },
        ]

    @staticmethod
    def _get_data_v202106():
        return [
            {
                'bbd_qyxx_id': 'ae906a1255cf4222805f2ca237d56c22', 'company_name': '广州市黄埔区智立科技开发部',
                'company_province': '广东', 'company_county': '440113'
            },
            {
                'bbd_qyxx_id': 'af789ac00fca4bd8a2daecf1fe3fa207', 'company_name': '北京市玉泉营瑞丰利装饰材料经营部',
                'company_province': '北京', 'company_county': '110106'
            },
            {
                'bbd_qyxx_id': 'ae6a0f7bd8b041999a16358514e48e10', 'company_name': '北京环京盛业电子产品中心',
                'company_province': '北京', 'company_county': '110106'
            },
            {
                'bbd_qyxx_id': 'b66777b2984147fa9d4736092c71c82b', 'company_name': '中国农业银行股份有限公司盘锦辽河行',
                'company_province': '辽宁', 'company_county': '211113'
            },
            {
                'bbd_qyxx_id': 'ae3c4f2570724a019ac2e68c5985fb01', 'company_name': '东乌旗兆泰科技',
                'company_province': '内蒙古', 'company_county': '152525'
            },
            {
                'bbd_qyxx_id': 'c9ebb111838f484bac1c08f7426c0d72', 'company_name': '中国邮政储蓄银行股份有限公司兴隆县大水泉营业所',
                'company_province': '河北', 'company_county': '130822'
            },
        ]

    def extract_data(self) -> DataFrame:

        self.logger.info(f"==== exec version is: {self.exec_version} ====")

        if self.exec_version == '202104':
            return self.spark.createDataFrame(self._get_data_v202104())

        if self.exec_version == '202105':
            return self.spark.createDataFrame(self._get_data_v202105())

        if self.exec_version == '202106':
            return self.spark.createDataFrame(self._get_data_v202106())

        return self.spark.createDataFrame(self._get_data_v202106())

    def meta_data(self) -> TableMetaData:
        return TableMetaData(
            source_table_name='qyxx_basic',
            destination_table_name=SubjectDataExtractTest.TABLE_NAME,
            destination_table_columns=SubjectDataExtractTest.TABLE_COLUMNS,
            primary_keys=['bbd_qyxx_id'],
            has_update_data=True
        )


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    subject_test = SubjectDataExtractTest(entry)
    subject_test.execute()


def post_check(entry: Entry):
    return True

#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-14 15:26
"""


class A:
    V1 = []

    @staticmethod
    def f1(*args, **kwargs):
        print(f"{args}, {kwargs}")

    @classmethod
    def f2(cls, *args, **kwargs):
        print(f"{cls}, {args}, {kwargs}")

    def f3(self, *args, **kwargs):
        print(f"{self}, {args}, {kwargs}")


def test_method_annotation_1():
    print()
    A().f1("a1", "a2", "a3", "a4")
    A().f2("a1", "a2", "a3", "a4")
    A().f3("a1", "a2", "a3", "a4")

    A.f1("a1", "a2", "a3", "a4")
    A.f2("a1", "a2", "a3", "a4")
    A.f3("a1", "a2", "a3", "a4")


def test_3():
    dict_date = {"pboc_corp_rcv_jn": '2021-12-01'}
    sql = f'''
                select 
                corp_code,
                sum(tx_amt_usd) as corp_inc_amt
                from pboc_corp_rcv_jn_v
                where rcv_date between add_months('{dict_date.get('pboc_corp_rcv_jn')}',-12) and '{dict_date.get('pboc_corp_rcv_jn')}'
                and tx_code = '121010'
                group by corp_code
                '''
    print(sql)
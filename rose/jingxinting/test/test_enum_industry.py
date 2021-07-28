#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: test_enum_industry
:Author: xufeng@bbdservice.com 
:Date: 2021-06-01 4:22 PM
:Version: v.1.0
:Description:
"""
from jingxinting.enums.enum_industry import Industry


def test_industry_enum():
    ret = Industry.ind_5p1_16p1_in_mapping("")
    print(ret)

    for ind in Industry.IND_ALL:
        print(ind.field_name)


if __name__ == "__main__":
    test_industry_enum()

# -*- coding: utf-8 -*-

import os


def py2():
    print('start processing...')
    os.system('python CQ_risk_detail_scoring.py')
    os.system('python CQ_invest_value_scoring.py')
    # os.system('python cqcyl_kjx_line.py')
    # os.system('python cqcyl_zsjz_line.py')
    # os.system('python cqcyl_zskxx_line.py')
    os.system('python main_model_zs.py')
    print('finished!')


def py3():
    print('start processing...')
    os.system('/opt/anaconda3/bin/python CQ_risk_detail_scoring.py')
    os.system('/opt/anaconda3/bin/python CQ_invest_value_scoring.py')
    # os.system('/opt/anaconda3/bin/python cqcyl_kjx_line.py')
    # os.system('/opt/anaconda3/bin/python cqcyl_zsjz_line.py')
    # os.system('/opt/anaconda3/bin/python cqcyl_zskxx_line.py')
    os.system('/opt/anaconda3/bin/python main_model_zs.py')
    print('finished!')


if __name__ == '__main__':
    py3()
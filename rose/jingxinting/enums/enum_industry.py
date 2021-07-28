#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: industry_industry
:Author: xufeng@bbdservice.com 
:Date: 2021-05-31 6:34 PM
:Version: v.1.0
:Description: 5+1　产业和　16+1 产业相关字段名和字段值
"""


class Ind5P1:

    def __init__(self, field_name, field_value, field_code):
        self.field_name = field_name
        self.field_value = field_value
        self.field_code = field_code
        self._children = set()

    def add_child(self, child):
        self._children.add(child)

    def all_child(self):
        return self._children


class Ind16P1:

    def __init__(self, field_name, field_value, parent_ind: Ind5P1, field_code):
        self.field_name = field_name
        self.field_value = field_value
        self.field_code = field_code
        self.parent = parent_ind
        self.parent.add_child(self)

    def ind_in(self, ind_txt):
        return self.field_value in ind_txt


class Industry:
    """
    ind_5_1_digital 数字经济	ind_16_1_digital 数字经济
    ind_5_1_equip 装备制造	ind_16_1_aviation 航空与燃机
    ind_5_1_equip 装备制造	ind_16_1_smart 智能装备
    ind_5_1_equip 装备制造	ind_16_1_orbital 轨道交通
    ind_5_1_equip 装备制造	ind_16_1_energy 新能源与智能汽车
    ind_5_1_energy 能源化工	ind_16_1_clean_energy 清洁能源
    ind_5_1_energy 能源化工	ind_16_1_chemical 绿色化工
    ind_5_1_energy 能源化工	ind_16_1_env 节能环保
    ind_5_1_material 先进材料	ind_16_1_material 新材料
    ind_5_1_elec 电子信息	ind_16_1_net 新一代网络技术
    ind_5_1_elec 电子信息	ind_16_1_soft 软件与信息服务
    ind_5_1_elec 电子信息	ind_16_1_bigdata 大数据
    ind_5_1_elec 电子信息	ind_16_1_circuit 集成电路与新型显示
    ind_5_1_food 食品饮料	ind_16_1_farm_prod 农产品精深加工
    ind_5_1_food 食品饮料	ind_16_1_liquor 优质白酒
    ind_5_1_food 食品饮料	ind_16_1_tea 精制川茶
    ind_5_1_food 食品饮料	ind_16_1_medical 医药健康
    """

    _IND_5P1_DIGITAL = Ind5P1('ind_5_1_digital', '数字经济', '0')
    _IND_5P1_ELEC = Ind5P1('ind_5_1_elec', '电子信息', '1')
    _IND_5P1_EQUIP = Ind5P1('ind_5_1_equip', '装备制造', '2')
    _IND_5P1_FOOD = Ind5P1('ind_5_1_food', '食品饮料', '3')
    _IND_5P1_MATERIAL = Ind5P1('ind_5_1_material', '先进材料', '4')
    _IND_5P1_ENERGY = Ind5P1('ind_5_1_energy', '能源化工', '5')

    _IND_16P1_DIGITAL = Ind16P1('ind_16_1_digital', '数字经济', _IND_5P1_DIGITAL, '0')
    _IND_16P1_SOFT = Ind16P1('ind_16_1_soft', '软件与信息', _IND_5P1_ELEC, '1')
    _IND_16P1_BIGDATA = Ind16P1('ind_16_1_bigdata', '大数据', _IND_5P1_ELEC, '2')
    _IND_16P1_CIRCUIT = Ind16P1('ind_16_1_circuit', '集成电路与新型显示', _IND_5P1_ELEC, '3')
    _IND_16P1_NET = Ind16P1('ind_16_1_net', '新一代网络', _IND_5P1_ELEC, '4')
    _IND_16P1_AVIATION = Ind16P1('ind_16_1_aviation', '航空与燃机', _IND_5P1_EQUIP, '5')
    _IND_16P1_SMART = Ind16P1('ind_16_1_smart', '智能装备', _IND_5P1_EQUIP, '6')
    _IND_16P1_ORBITAL = Ind16P1('ind_16_1_orbital', '轨道交通', _IND_5P1_EQUIP, '7')
    _IND_16P1_ENERGY = Ind16P1('ind_16_1_energy', '能源与智能汽车', _IND_5P1_EQUIP, '8')
    _IND_16P1_FARM_PROD = Ind16P1('ind_16_1_farm_prod', '农产品精深加工', _IND_5P1_FOOD, '9')
    _IND_16P1_LIQUOR = Ind16P1('ind_16_1_liquor', '优质白酒', _IND_5P1_FOOD, '10')
    _IND_16P1_TEA = Ind16P1('ind_16_1_tea', '精制川茶', _IND_5P1_FOOD, '11')
    _IND_16P1_MEDICAL = Ind16P1('ind_16_1_medical', '医药健康', _IND_5P1_FOOD, '12')
    _IND_16P1_MATERIAL = Ind16P1('ind_16_1_material', '新材料', _IND_5P1_MATERIAL, '13')
    _IND_16P1_CLEAN_ENERGY = Ind16P1('ind_16_1_clean_energy', '清洁能源', _IND_5P1_ENERGY, '14')
    _IND_16P1_CHEMICAL = Ind16P1('ind_16_1_chemical', '绿色化工', _IND_5P1_ENERGY, '15')
    _IND_16P1_ENV = Ind16P1('ind_16_1_env', '节能环保', _IND_5P1_ENERGY, '16')

    IND_5P1_ALL = [_IND_5P1_DIGITAL, _IND_5P1_EQUIP, _IND_5P1_ENERGY, _IND_5P1_MATERIAL, _IND_5P1_ELEC, _IND_5P1_FOOD]
    IND_16P1_ALL = [
        _IND_16P1_DIGITAL, _IND_16P1_AVIATION, _IND_16P1_SMART, _IND_16P1_ORBITAL,
        _IND_16P1_ENERGY, _IND_16P1_CLEAN_ENERGY, _IND_16P1_CHEMICAL, _IND_16P1_ENV,
        _IND_16P1_MATERIAL, _IND_16P1_NET, _IND_16P1_SOFT, _IND_16P1_BIGDATA,
        _IND_16P1_CIRCUIT, _IND_16P1_FARM_PROD, _IND_16P1_LIQUOR, _IND_16P1_TEA, _IND_16P1_MEDICAL
    ]
    IND_ALL = IND_5P1_ALL + IND_16P1_ALL
    CODE_MAPPING_16P1 = {ind.field_code: ind for ind in IND_16P1_ALL}

    @staticmethod
    def all_5p1_field():
        """
        所有5+1产业所有对应的字段的字段名
        :return:
        """
        return [ind.field_name for ind in Industry.IND_5P1_ALL]

    @staticmethod
    def all_16p1_field():
        """
        所有16+1产业所对应字段的字段名
        :return:
        """
        return [ind.field_name for ind in Industry.IND_16P1_ALL]

    @staticmethod
    def all_industry_field():
        """
        5+1产业和16+1产业的所有字段字段名
        :return:
        """
        return [ind.field_name for ind in Industry.IND_ALL]

    @staticmethod
    def ind_16p1_in(ind_txt):
        """
        文本包含的所有16+1产业的字段
        :param ind_txt:
        :return:
        """
        return [ind.field_name for ind in Industry.IND_16P1_ALL if ind.ind_in(ind_txt)]

    @staticmethod
    def ind_5p1_16p1_in_mapping(ind_txt):
        """
        根据文本判断所在的5+1产业和16+1产业的结果映射:返回各个5+1和16+1产业的字段名和字段值
        :param ind_txt:
        :return:
        """
        mapping = {ind.field_name: '0' for ind in Industry.IND_ALL}
        for ind in Industry.IND_16P1_ALL:
            if ind.ind_in(ind_txt):
                mapping[ind.field_name] = '1'
                mapping[ind.parent.field_name] = '1'
        return mapping

    @staticmethod
    def ind_5p1_16p1_code_mapping(lst_16p1_code):
        ret_dict = dict()
        for ind_code in lst_16p1_code:
            ind = Industry.CODE_MAPPING_16P1.get(ind_code, None)
            if not ind:
                continue
            ret_dict[ind.field_name] = '1'
            ret_dict[ind.parent.field_name] = '1'
        return ret_dict

    @staticmethod
    def all_child_field(ind_field_name_value):
        """
        根据5+1产业的字段名或中文解释找到对应的子产业的所有字段
        :param ind_field_name_value:
        :return:
        """
        children = Industry._all_child(ind_field_name_value)
        return [ch.field_name for ch in children]

    @staticmethod
    def all_child_value(ind_field_name_value):
        children = Industry._all_child(ind_field_name_value)
        return [ch.field_value for ch in children]

    @staticmethod
    def ind_16p1_desc_code_map():
        return {item.field_value: int(item.field_code) for item in Industry.IND_16P1_ALL}

    @staticmethod
    def _all_child(ind_field_name_value):
        for ind in Industry.IND_5P1_ALL:
            if ind_field_name_value in [ind.field_name, ind.field_value]:
                return ind.all_child()
        return []

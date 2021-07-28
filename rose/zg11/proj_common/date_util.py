#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: date_util
:Author: xufeng@bbdservice.com 
:Date: 2021-04-21 10:58 AM
:Version: v.1.0
:Description: from hainan-fta date util
"""
import datetime
from dateutil.parser import parse as date_parse
from dateutil.relativedelta import relativedelta


class DateUtils:

    @classmethod
    def now2str(cls, fmt="%Y-%m-%d"):
        return cls.date2str(datetime.datetime.now(), fmt)

    @staticmethod
    def date2str(date_time: datetime.datetime, fmt="%Y-%m-%d"):
        return date_time.strftime(fmt)

    @staticmethod
    def str2date(date_str, fmt=None):
        if fmt:
            return datetime.datetime.strptime(date_str, fmt)
        else:
            return date_parse(date_str)

    @staticmethod
    def add_date(
            date_time: datetime.datetime,
            years=0, months=0, days=0, leapdays=0, weeks=0,
            hours=0, minutes=0, seconds=0, microseconds=0
    ):
        """
        在给定时间基础上  增加 年月日时分秒 时长
        :param date_time: 空则基础当前时间
        :param years:
        :param months:
        :param days:
        :param leapdays:
        :param weeks:
        :param hours:
        :param minutes:
        :param seconds:
        :param microseconds:
        :return:
        """
        if date_time and not isinstance(date_time, datetime.datetime):
            raise Exception("date_time parm is must be datetime.datetime")
        date_time = date_time or datetime.datetime.now()
        return date_time + relativedelta(
            years=years, months=months, days=days, leapdays=leapdays, weeks=weeks,
            hours=hours, minutes=minutes, seconds=seconds, microseconds=microseconds
        )

    @classmethod
    def add_date_2str(
            cls,
            date_time: datetime.datetime, fmt="%Y-%m-%d",
            years=0, months=0, days=0, leapdays=0, weeks=0,
            hours=0, minutes=0, seconds=0, microseconds=0
    ):
        """
        1、在给定时间基础上  增加 年月日时分秒 时长
        2、结果换为字符串
        :param date_time: 空则基础当前时间
        :param years:
        :param months:
        :param days:
        :param leapdays:
        :param weeks:
        :param hours:
        :param minutes:
        :param seconds:
        :param microseconds:
        :param fmt: %Y-%m-%d %H:%M:%S
        :return:
        """
        if date_time and not isinstance(date_time, datetime.datetime):
            raise Exception("date_time parm is must be datetime.datetime")
        r_datetime = cls.add_date(
            date_time,
            years, months, days, leapdays, weeks,
            hours, minutes, seconds, microseconds
        )
        return r_datetime.strftime(fmt)

    @classmethod
    def add_date_2str_4str(
            cls,
            date_time: str, in_fmt="%Y-%m-%d", out_fmt="%Y-%m-%d",
            years=0, months=0, days=0, leapdays=0, weeks=0,
            hours=0, minutes=0, seconds=0, microseconds=0
    ):
        """
        1、在给定时间字符串 转 时间
        2、时间基础上  增加 年月日时分秒 时长
        2、结果时间格式化字符串
        :param date_time: 空则基础当前时间
        :param years:
        :param months:
        :param days:
        :param leapdays:
        :param weeks:
        :param hours:
        :param minutes:
        :param seconds:
        :param microseconds:
        :param in_fmt: %Y-%m-%d %H:%M:%S
        :param out_fmt: %Y-%m-%d %H:%M:%S
        :return:
        """
        if date_time and not isinstance(date_time,str):
            raise Exception("date_time parm is must be string")
        z_time = datetime.datetime.strptime(date_time,in_fmt) if date_time else None
        r_datetime = cls.add_date_2str(
            z_time, out_fmt,
            years, months, days, leapdays, weeks,
            hours, minutes, seconds, microseconds
        )
        return r_datetime

    @classmethod
    def add_date_4str(
            cls,
            date_time: str, in_fmt="%Y-%m-%d",
            years=0, months=0, days=0, leapdays=0, weeks=0,
            hours=0, minutes=0, seconds=0, microseconds=0
    ):
        """
        1、在给定时间字符串 转 时间
        2、时间基础上  增加 年月日时分秒 时长
        :param date_time: 空则基础当前时间
        :param years:
        :param months:
        :param days:
        :param leapdays:
        :param weeks:
        :param hours:
        :param minutes:
        :param seconds:
        :param microseconds:
        :param in_fmt: %Y-%m-%d %H:%M:%S
        :return:
        """
        if date_time and not isinstance(date_time, str):
            raise Exception("date_time parm is must be string")
        z_time = datetime.datetime.strptime(date_time, in_fmt) if date_time else None
        r_datetime = cls.add_date(
            z_time,
            years, months, days, leapdays, weeks,
            hours, minutes, seconds, microseconds
        )
        return r_datetime

    @staticmethod
    def diff_date(d1, d2):
        """
        两时间差  返回  年月日周 之一
        *注意：返回值为四舍五入整数
        :param d1:
        :param d2:
        :return:
        """
        if not isinstance(d1, datetime.datetime) or not isinstance(d2, datetime.datetime):
            raise Exception("d1,d2 parm is must be datetime")

        if d1 > d2:
            one_day = d2
            today = d1
        else:
            one_day = d1
            today = d2

        rr = relativedelta(today, one_day)

        return rr.years, rr.months, rr.days, (today - one_day).days

    @classmethod
    def date_diff_4str(cls, d1, d2, fmt="%Y-%m-%d"):
        """
        根据传入的时间字符串判定两者的时间差，返回 (年, 月, 日,　周) 四元组
        :param d1:
        :param d2:
        :param fmt:
        :return:
        """
        if not isinstance(d1, str) or not isinstance(d2, str):
            raise Exception("d1,d2 parm is must be string")
        zd1 = datetime.datetime.strptime(d1, fmt)
        zd2 = datetime.datetime.strptime(d2, fmt)
        return cls.diff_date(zd1, zd2)


if __name__ == '__main__':
    pass

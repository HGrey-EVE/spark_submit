# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 

Date: 2021/7/20 15:51
"""

import smtplib
import os
from collections import namedtuple
from email.message import EmailMessage
import mimetypes
from typing import List, Text
from datetime import date, datetime
import pandas
import subprocess
import time

# ------------- Models ------------------
from whetstone.core.entry import Entry

EmailProvider = namedtuple("EmailProvider", "host port")
EmailAccount = namedtuple("EmailAccount", "username password")


# hdfs上csv文件存放路径
# CSV_PATH = "/user/zszqyuqing/zszq/yq_v1"
# 服务器上csv文件存放路径
# STORE_PATH = "/data2/zszqyuqing/yq_v1/data"
# 将今天时间格式化成指定形式
# TODAY = date.today().strftime("%Y%m%d")


class EmailAddress(namedtuple("EmailAddress", "title address")):
    def __repr__(self):
        return f"{self.title} <{self.address}>"


class SendEmail:
    '''
    module:对应conf里的hdfs-biz，传 yq_v1、yq_v2、gg
    receivers：收件人，是个嵌套的list,如：LIST[['名字','邮箱'],....]
    cc_receivers:抄送，格式同上
    '''

    def __init__(self, entry: Entry, module, receivers: list, cc_receivers: List):
        self.module = module
        self.version = entry.version
        self.file_name = f"{module}_{self.version}.csv"
        self.hdfs_path_file = entry.cfg_mgr.hdfs.get_result_path('hdfs-biz', f'{module}_data')
        self.local_path = f"/data2/zszqyuqing/{self.version}/{module}"
        self.email_provider = EmailProvider("smtp.bbdservice.com", 25)
        self.email_account = EmailAccount("bigdata_system@bbdservice.com", "Bigdata_system")
        self.sender = EmailAddress("数据研发中心", "bigdata_system@bbdservice.com")
        try:
            self.receivers = [EmailAddress(f"{x[0]}", f"{x[1]}") for x in receivers]
            self.cc_receivers = [EmailAddress(f"{x[0]}", f"{x[1]}") for x in cc_receivers]
        except IndexError:
            entry.logger.info('主送或者抄送为空')

    def send_email_all(self):
        if 'v1' in self.module:
            module = 'v1'
        elif 'v2' in self.module:
            module = 'v2'
        else:
            module = '公告'
        get_hdfs_csv(hdfs_path_file=self.hdfs_path_file, local_path=self.local_path, file_name=self.file_name)
        client = smtp_client_factory(self.email_provider, self.email_account)
        csv_count = get_csv_count(path=f"{self.local_path}/{self.file_name}")
        subject = f"招商证券投研分析平台项目-{self.version}-舆情_{module}数据导出"
        content = "@黄尧、陈小伟、敖日格勒、叶胜兰:\n" \
                  f"     今天的舆情_{module}数据已导出，共{csv_count}条，详情见附件，请查收，谢谢"
        send_mail(
            client,
            assemble_attachment(assemble_email(
                self.sender, self.receivers, self.cc_receivers, subject=subject, content=content
            ), f'{self.file_name}', f'{self.local_path}/{self.file_name}')
        )


def smtp_client_factory(
        mail_provider: EmailProvider, account: EmailAccount
) -> smtplib.SMTP:
    client = smtplib.SMTP(host=mail_provider.host, port=mail_provider.port)
    client.login(account.username, account.password)
    return client


def assemble_email(
        mail_from: EmailAddress, mail_to: List[EmailAddress], mail_cc: List[EmailAddress], subject: Text,
        content: Text
) -> EmailMessage:
    msg = EmailMessage()
    msg.set_content(content)
    msg["Subject"] = subject
    msg["From"] = str(mail_from)
    msg["To"] = ", ".join(str(it) for it in mail_to)
    msg["CC"] = ", ".join(str(it) for it in mail_cc)
    msg
    # print(msg)
    return msg


def assemble_attachment(msg: EmailMessage, filename: Text, path: Text):
    if not os.path.isfile(path):
        raise ValueError(f"attachment is not a file")
    # Guess the content type based on the file's extension.  Encoding
    # will be ignored, although we should check for simple things like
    # gzip'd or compressed files.
    ctype, encoding = mimetypes.guess_type(path)
    if ctype is None or encoding is not None:
        # No guess could be made, or the file is encoded (compressed), so
        # use a generic bag-of-bits type.
        ctype = "application/octet-stream"
    maintype, subtype = ctype.split("/", 1)
    with open(path, "rb") as fp:
        msg.add_attachment(
            fp.read(), maintype=maintype, subtype=subtype, filename=filename
        )
    return msg


def send_mail(client: smtplib.SMTP, msg: EmailMessage):
    client.send_message(msg)


# 将hdfs上的文件拉取到服务器
def get_hdfs_csv(hdfs_path_file, local_path, file_name):
    print('进入get_hdfs_csv...')
    os.system(f"mkdir -p {local_path}")
    os.system(f"hadoop fs -get {hdfs_path_file}/*.csv {local_path}/{file_name}")


# 获取csv文件条数
def get_csv_count(path):
    df = pandas.read_csv(path, error_bad_lines=False)
    return df.shape[0]

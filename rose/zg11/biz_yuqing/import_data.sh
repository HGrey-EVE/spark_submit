#!/bin/bash

datetime=`date +%Y%m%d` #获取当前时间20201204
now_date=`date +%Y-%m-%d`
old_date=`date -d "1 day ago" +"%Y-%m-%d"`
start_time=${old_date}' 16:00:00'
end_time=${now_date}' 16:00:00'

import_data(){
 sqoop \
 import -D mapreduce.job.queuename=vip.zszqyuqing \
 --connect jdbc:mysql://bbdhiggs.mysql.bbdops.com:53606/bbd_dp_datacube \
 --username bbd_dw_read \
 --password 'QTdnYezNo571kmZG0liP' \
 --as-textfile \
 --target-dir /user/hive/warehouse/pj.db/zg11_zszq_yuqing/dt=${datetime} \
 --delete-target-dir \
 --fields-terminated-by '\001' \
 --null-string '\\N' \
 --null-non-string '\\N' -m 10 \
 --hive-drop-import-delims \
 --split-by event_type \
 --query "$1 and \$CONDITIONS"
}

import_zszq_info(){
  import_data "select zy.bbd_xgxx_id,zye.event_subject,zye.event_object,zy.news_title,zy.main,zy.source_url,zy.news_site,zye.event_type,zy.pubdate,zy.ctime from news_zszq_event zye inner join news_zszq zy on zye.bbd_xgxx_id=zy.bbd_xgxx_id and zye.pubdate between '${start_time}' and '${end_time}' "
}

# 将MySQL数据导入hdfs
import_zszq_info

# 修复表，加载数据
hive -e 'msck repair table pj.zg11_zszq_yuqing'

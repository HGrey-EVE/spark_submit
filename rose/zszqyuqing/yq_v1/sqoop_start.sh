#!/bin/bash

datetime=`date +%Y%m%d` #获取当前时间20201204
now_date=`date +%Y-%m-%d`
old_date=`date -d "1 day ago" +"%Y-%m-%d"`
start_time=${old_date}' 16:20:00'
end_time=${now_date}' 16:20:00'

import_data(){
 sqoop import -D mapreduce.job.queuename=vip.zszqyuqing --connect jdbc:mysql://bbdhiggs.mysql.bbdops.com:53606/bbd_dp_datacube --username bbd_dw_read --password 'QTdnYezNo571kmZG0liP' --as-textfile --target-dir /user/hive/warehouse/pj.db/ms_ipo_raw_event_new/dt=${datetime} --delete-target-dir --fields-terminated-by '\001' --null-string '\\N' --null-non-string '\\N' -m 1 --hive-drop-import-delims --split-by auto_inc --query "$1 and \$CONDITIONS"
}

import_zszq_info(){
  import_data "select zye.auto_inc,zye.event_subject,zye.subject_id,zye.event_object,zye.object_id,zye.event_type,zy.bbd_url,zye.bbd_unique_id,zye.search_id,zy.main from news_zszq_event zye left join news_zszq zy on zye.bbd_xgxx_id=zy.bbd_xgxx_id where  (zy.news_type=1 or zy.news_type=2) and zye.pubdate>='${start_time}' and zye.pubdate<'${end_time}'"
}

# 将MySQL数据导入hdfs
import_zszq_info

# 修复表
hive -e 'msck repair table pj.ms_ipo_raw_event_new'
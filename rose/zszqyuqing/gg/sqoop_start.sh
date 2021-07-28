#!/bin/bash

datetime=`date +%Y%m%d` #获取当前时间20201204
now_date=`date +%Y-%m-%d`
old_date=`date -d "1 day ago" +"%Y-%m-%d"`
start_time=${old_date}' 16:20:00'
end_time=${now_date}' 16:20:00'

import_data(){
 sqoop import -D mapreduce.job.queuename=vip.zszqyuqing --connect jdbc:mysql://bbdhiggs.mysql.bbdops.com:53606/bbd_dp_datacube --username bbd_dw_read --password 'QTdnYezNo571kmZG0liP' --as-textfile --target-dir /user/hive/warehouse/pj.db/gg_event/dt=${datetime} --delete-target-dir --fields-terminated-by '\001' --null-string '\\N' --null-non-string '\\N' -m 1 --hive-drop-import-delims --split-by auto_inc --query "$1 and \$CONDITIONS "
}

import_zszq_info(){
  import_data "select gge.auto_inc,gg.stock_name,gge.event_subject,gge.subject_id,gg.news_type,gge.event_object,gge.object_id,gge.event_type,gg.bbd_url,gge.bbd_unique_id,gge.search_id from news_zszq_gg_event gge left join news_zszq_gg gg on gge.bbd_xgxx_id=gg.bbd_xgxx_id where (gg.news_type=5 or gg.news_type=6) and gg.pubdate>='${start_time}' and gg.pubdate<'${end_time}'"
}

# 将MySQL数据导入hdfs
import_zszq_info

# 修复表
hive -e 'msck repair table pj.gg_event'
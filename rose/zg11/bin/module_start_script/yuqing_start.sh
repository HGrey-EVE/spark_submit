#!/bin/sh

curday=`date +%Y%m%d`

set -e
/bin/sh /data4/bf-zg11/zg11_code/spark_plan_zero/rose/zg11/biz_zszq_yuqing/import_data.sh > import_mysql_debug.log 2>&1
/opt/anaconda3/bin/python /data4/bf-zg11/zg11_code/spark_plan_zero/stone/src/whetstone/main.py -p zg11 -m biz_yuqing -t yuqing_excute -v $curday -d -e dev > yuqing_excute_debug.log 2>&1
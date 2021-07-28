#!/bin/sh
cur_date=`date +"%Y%m%d"`
set -e
/bin/sh /data2/zszqyuqing/test/hgy/spark_plan_zero/rose/zszqyuqing/gg/sqoop_start.sh > sqoop_gg_debug.log 2>&1
wait
sleep 5s
/opt/anaconda3/bin/python /data2/zszqyuqing/test/hgy/spark_plan_zero/stone/src/whetstone/main.py -p zszqyuqing -m gg -t gg -v $cur_date -d -e dev > gg_debug.log 2>&1

/opt/anaconda3/bin/python /data2/zszqyuqing/test/hgy/spark_plan_zero/stone/src/whetstone/main.py -p zszqyuqing -m gg -t gg -v 20210723-d -e dev
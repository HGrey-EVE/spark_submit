#!/bin/sh
cur_date=`date +"%Y%m%d"`
/opt/anaconda3/bin/python  /data2/zszqyuqing/spark_plan_zero/stone/src/whetstone/main.py -p zszqyuqing -m guanlianfang -t guanlianfang -v $cur_date -d -e dev > guanlianfang_debug.log 2>&1
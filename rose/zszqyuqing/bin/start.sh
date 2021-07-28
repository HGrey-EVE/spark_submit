#!/bin/bash
curday=`date -d "last month" +%Y%m`
set -e
/opt/anaconda3/bin/python /data4/bf-zg11/zg11_code/spark_plan_zero/stone/src/whetstone/main.py -p zszqyuqing -m gg -t entity_etl -v $curday -d -e dev > entity_etl_debug.log 2>&1
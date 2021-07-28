#!/bin/sh
curday=`date -d "last month" +%Y%m`
set -e
nohup /opt/anaconda3/bin/python /data4/bf-zg11/lirenchao/spark_plan_zero/stone/src/whetstone/main.py -p zg11 -m biz_entity -t entity_etl -v $curday -d -e dev > entity_etl_debug.log 2>&1 &

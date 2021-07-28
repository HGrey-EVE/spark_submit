#!/bin/sh
curday=`date -d "last month" +%Y%m`
set -e
/opt/anaconda3/bin/python /data2/commondataexport/hougy/spark_plan_zero/stone/src/whetstone/main.py -p data_export -m commonexport -t xuqiu1 -v $curday -d -e dev
/opt/anaconda3/bin/python /data2/commondataexport/hougy/spark_plan_zero/stone/src/whetstone/main.py -p data_export -m commonexport -t xuqiu2 -v $curday -d -e dev





/opt/anaconda3/bin/python /data2/commondataexport/hougy/spark_plan_zero/stone/src/whetstone/main.py -p data_export -m commonexport -t xuqiu1 -v 20210722 -d -e dev
/opt/anaconda3/bin/python /data2/commondataexport/hougy/spark_plan_zero/stone/src/whetstone/main.py -p data_export -m commonexport -t xuqiu2 -v 20210726 -d -e dev
/opt/anaconda3/bin/python /data2/commondataexport/hougy/spark_plan_zero/stone/src/whetstone/main.py -p data_export -m commonexport -t xuqiu3 -v 20210726 -d -e dev








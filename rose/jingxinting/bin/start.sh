set -e
curday=`date -d "last month" +%Y%m`
/opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_company_tags -t company_tags -v $curday -d -e dev
/opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_base_data -t basic_main -v $curday -d -e dev
set +e
/opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_city_development -t city_count -v $curday -d -e dev
/opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_domestic_compare -t domestic_count -v $curday -d -e dev
/opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_industry_5_p_1 -t biz_5_p_1_count -v $curday -d -e dev
/opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_industry_5_p_1 -t biz_5_p_1_tradition_count -v $curday -d -e dev
/opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_industry_5_p_1 -t biz_5p1_industry_map -v $curday -d -e dev
/opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_park_analysis -t park_analysis -v $curday -d -e dev
/opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_enterprise_monitor -t enterprise_dev_index -v $curday -d -e dev
/opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_industry_finance -t industry_finance -v $curday -d -e dev

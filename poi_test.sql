create table SERVICE_SECURITY.poi_test
(
city_name STRING comment'城市名',
name STRING  comment'起终点名字',
type STRING comment'POI类型'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
location 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/poi_test'


#!/bin/bash
start=$1
start1=`date -d "1 days ago  $start" +%Y-%m-%d` 
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;
set hive.auto.convert.join=true;
set hive.exec.max.dynamic.partitions.pernode=100000;

INSERT OVERWRITE TABLE service_security.poi_test 
PARTITION(year,month,day) 
SELECT
updata.city_name, 
updata.name,
updata.type,
'$years' as year,
'$months' as month,
'$days' as day
FROM
(
select a1.name,b1.type from
(
select b.order_id,b.type from
(
select order_id,starting_poi_type as type  from service_security.poi_normal 
where concat_ws('-',year,month,day)='$start'
union
select order_id,dest_poi_type as type from service_security.poi_normal 
where concat_ws('-',year,month,day)='$start'
)b
)b1
join
(
select a.order_id, a.name,a.city_name from
(
select order_id,starting_name as name,city_name  from gulfstream_dw.dw_v_order_base 
where concat_ws('-',year,month,day)='$start' and starting_name is not null
union 
select order_id,dest_name as name,city_name from gulfstream_dw.dw_v_order_base 
where concat_ws('-',year,month,day)='$start' and dest_name is not null
)a)a1
on a1.order_id=b1.order_id
UNION ALL
SELECT city_name,name,type FROM  service_security.poi_test
where concat_ws('-',year,month,day)='$start1'
)updata GROUP BY updata.name,updata.type
distribute by(year,month,day);"
create table service_security.dis_time
(
order_id bigint COMMENT'订单ID ',
driver_id bigint COMMENT'司机ID',
passenger_id bigint COMMENT'乘客ID',
distance_rate double COMMENT'实际行驶距离/预估路面距离',
time_rate double COMMENT'实际行驶时间/预估行驶时间'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/dis_time';


Create table dis_time_15 like service_security.dis_time
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/dis_time_15';


 #!/bin/bash
start=$1
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`
hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.dis_time
PARTITION(year,month,day)
select
order_id,
driver_id,
passenger_id,
round(cast(start_dest_distance/1000.0 as double)/normal_distance ,4)as distance_rate,
round(cast( (unix_timestamp(finish_time)-unix_timestamp(begin_charge_time) )/60 as double)/forecast_time,4) as time_rate,
'$years' as year,
'months' as month,
'days' as day
from gulfstream_dw.dw_v_order_base --订单基础表
where concat_ws('-',year,month,day)='$start' and finish_time!='0000-00-00 00:00:00' and begin_charge_time !='0000-00-00 00:00:00'
and normal_distance!=0
distribute by (year,month,day);"

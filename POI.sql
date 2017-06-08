Create table Service_security.POI
(
dest_district_code  string              COMMENT'终点行政区编码',
dest_district_name  string              COMMENT'终点行政区名称',
dest_famous_area    string              COMMENT'终点商圈 ',
dest_poi_type       string              COMMENT'终点POI类型',
need_order_id       string              COMMENT'去重后订单ID',
order_id            string              COMMENT'订单ID',
starting_district_code string              COMMENT'起点行政区编码',
starting_district_name string              COMMENT'起点行政区名称',
starting_famous_area string              COMMENT'起点商圈 ',
starting_poi_type   string              COMMENT'起点POI类型'
)
PARTITIONED BY (
  `year` string,
  `month` string,
  `day` string)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/data/service_security/POI';




 #!/bin/bash
start=$1
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`
hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.POI
PARTITION(year,month,day)
select
b.dest_district_code,
b.dest_district_name,
b.dest_famous_area,
b.dest_poi_type,
b.need_order_id,
a.id,
b.starting_district_code,
b.starting_district_name,
b.starting_famous_area,
b.starting_poi_type
from
 (
 select dest_district_code,dest_district_name,dest_famous_area,dest_poi_type,need_order_id,order_id,
 starting_district_code,starting_district_name,starting_famous_area,starting_poi_type
 from decision.fex_d5031d_160 where concat_ws('-',year,month,day)='$start'
 )b
 join
 (
 select param['bubble_id'] as order_id,param['oid'] as id from gulfstream_ods. g_order_create
 where  concat_ws('-',year,month,day)='$start'  and param['bubble_id'] is not null and param['oid'] is not null
 )a
 on a.order_id=b.order_id
 join
 (
 select order_id from service_security.major_complaint_info where  concat_ws('-',year,month,day)='$start'
 )c
 on c.order_id=a.id
 distribute by(year,month,day);"

 select *
 from
 (
 select id,val from test
 )a
left outer join
 (select id,val from test_a)b
 on a.id=b.id
 union all
 select *
 from
 (
 select id,val from test
 )c
right outer join
 (select id,val from test_a)d
 on c.id=d.id

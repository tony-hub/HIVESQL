Create table Service_security.POI_NORMAL
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
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/POI_NORMAL'



LOCATION 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/poi'
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





 ==========================================
#!/bin/bash
start=$1
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.poi
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
b.starting_poi_type,
'$years' as year,
'$months' as month,
'$days' as day

select *
from
(
select order_id from service_security.major_complaint where  concat_ws('-',year,month,day)='2016-12-01'
)c
join
(
select param['bubble_id'] as order_id,param['oid'] as id from gulfstream_ods.g_order_create
where  concat_ws('-',year,month,day)='2016-12-01'  and param['bubble_id'] is not null and param['oid'] is not null
)a
on c.order_id=a.id
join
(
select dest_district_code,dest_district_name,dest_famous_area,dest_poi_type,need_order_id,order_id,starting_district_code,
starting_district_name,starting_famous_area,starting_poi_type from decision.fex_d5031d_160
where concat_ws('-',year,month,day)='$start'
)b
on a.order_id=b.order_id

distribute by(year,month,day);
dfs -rm -r  hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/*/.hive*;
dfs -rm -r  hdfs://mycluster-tj/user/common_plat_security/.Trash;
"
plutus:estimate:4a65ca71-1ab3-4d5c-570a-26e0ad50bef7
select dest_district_code,dest_district_name,dest_famous_area,dest_poi_type,need_order_id,order_id,starting_district_code,
starting_district_name,starting_famous_area,starting_poi_type from decision.fex_d5031d_160
where concat_ws('-',year,month,day)='2016-12-01' and order_id='plutus:estimate:4a65ca71-1ab3-4d5c-570a-26e0ad50bef7'


Ubiquitous in-network caching is one of key features of Information
Centric Network, together with receiver-drive content retrieval paradigm, Information
Centric Network is better support for content distribution, multicast, mobility,
etc. Cache placement strategy is crucial to improving utilization of cache
space and reducing the occupation of link bandwidth. Most of the literature about
caching policies considers the overall cost and bandwidth, but ignores the limits
of node cache capacity. This paper proposes a G-FMPH algorithm which takes
into account both constrains on the link bandwidth and the cache capacity of
nodes. Our algorithm aims at minimizing the overall cost of contents caching
afterwards. The simulation results have proved that our proposed algorithm has
a better performance.

dest_district_code	dest_district_name	dest_famous_area	dest_poi_type	need_order_id	order_id	starting_district_code	starting_district_name	starting_famous_area	starting_poi_type
x.bubble_id,x.need_bubble_id,x.starting_famous_area,x.starting_district_name,x.starting_district_code,x.dest_famous_area,x.dest_district_name,x.dest_district_code,x.starting_poi_type,x.dest_poi_type

select get_json_object(‘${hivevar:msg}’,’$.server’)
set hivevar:msg={"message":"2015/12/08 09:14:4", "client": "10.108.24.253", "server": "passport.suning.com", 
"request": "POST /ids/needVerifyCode HTTP/1.1",
"server": "passport.sing.co",
"version":"1",
"timestamp":"2015-12-08T01:14:43.273Z",
"type":"B2C","center":"JSZC",
"system":"WAF","clientip":"192.168.61.4",
"host":"wafprdweb03",
"path":"/usr/local/logs/waf.error.log",
"redis":"192.168.24.46"}
Create table service_security.app_sensitive1
(
id string COMMENT'1:社交,2:色情,3:借贷,4:直播,5:视频,0:其他',
app_name STRING COMMENT'敏感APP的名字'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/app_sensitive1'


 select * from driver_app where installed_app like concat('%','','%') limit 10;

Create table service_security.driver_applist(
driver_id BIGINT COMMENT'ID',
brand STRING COMMENT'手机品牌',
Installed_app string COMMENT'安装的APP列表',
id string COMMENT'1：社交直播（偏色情）2：色情 3：借贷',
cnt int COMMENT 'N种敏感APP'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/driver_applist'

Create table service_security.driver_applist_bak(
driver_id BIGINT COMMENT'ID',
brand STRING COMMENT'手机品牌',
Installed_app string COMMENT'安装的APP列表',
id string COMMENT'1：社交直播（偏色情）2：色情 3：借贷'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/driver_applist_bak'

=======================================================
insert into table service_security.driver_applist
select e.driver_id,e.brand,concat_ws(',',collect_set(e.app)) as app,
concat_ws(',',collect_set(cast(e.id as string))) as classid
from
(
select distinct c.driver_id,c.brand,c.app,d.id from
(
select a.driver_id, a.brand, trim(app) as app from driver_app a
lateral view explode(split(a.installed_app,','))b as app
)c
left outer join
(
select id,app_name from app_sensitive
)d
ON(TRUE)
WHERE instr(c.app,d.app_name)>0
)e group by e.driver_id,e.brand,e.app,e.id;

 select * from driver_app a  lateral view explode(split(a.installed_app,','))b as app;
==============================================================================
insert into table service_security.driver_applist
select e.driver_id,e.brand,e.installed_app,
concat_ws(',',collect_set(cast(e.id as string))) as classid
from
(
select * ,Row_Number() OVER (partition by driver_id ORDER BY id ) from
(
select distinct c.driver_id,c.brand,c.installed_app,
(case when instr(c.installed_app,d.app_name)>0 then d.id
else  null end) as id
from
(
select driver_id, brand, installed_app from driver_app
)c
left outer join
(
select id,app_name from app_sensitive
)d
ON(TRUE)
)f
)e group by e.driver_id,e.brand,e.installed_app
;
==========================hive=========================

hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set mapred.reduce.tasks=40;

insert into table service_security.driver_applist_bak
select e.driver_id,e.brand,e.installed_app,
concat_ws(',',collect_set(cast(e.id as string))) as classid
from
(
select * ,Row_Number() OVER (partition by driver_id ORDER BY id ) from
(
select distinct c.driver_id,c.brand,c.installed_app,
(case when instr(c.installed_app,d.app_name)>0 then d.id else null end) as id
from
(
select driver_id, brand, installed_app from service_security.driver_app
)c
left outer join
(
select id,app_name from service_security.app_sensitive
)d
ON(TRUE)
)f
)e group by e.driver_id,e.brand,e.installed_app;


insert into table service_security.driver_applist
select
c.driver_id,
c.brand,
c.Installed_app,
(case when length(c.id) =0 then 0 else c.id end) as id,
(case when length(c.id)=0 then  0 else count(c.classid)  end)as cnt
from(
select * from  driver_applist_bak a lateral view explode(split(a.id,',')) b as classid
)c
group by c.driver_id,c.brand,c.installed_app,c.id;
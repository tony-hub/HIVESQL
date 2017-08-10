create table service_security.h_z(
driver_id BIGINT COMMENT'司机ID',
city_name STRING COMMENT'城市名',
accdnt_score  float COMMENT'重大投诉评分',
conflict_score float COMMENT'车内冲突评分',
sexualharass_score float  COMMENT'性骚扰评分'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/h_z'


insert overwrite table  service_security.h_z
select 
driver_id,
city_name,
d_accdnt_score,
d_conflict_score,
d_sexualharass_score
from major_com_y where city_name like '%杭州%'
and (path like "%性骚扰%")  and path not like '%顺风车%'
and path not like '%交通事故%' and path not like '%失联%' and path not like '%延误行程%' 
and path not like '%出租车%' and product_id in (1,2,6,3,4,7)
and content  not like '%司机描述%'
and (order_id is not null) and length(cast(order_id as string))>=6;
0点
3点
两周

insert overwrite table  service_security.h_z
select * from h_z order by accdnt_score desc
insert overwrite table service_security.h_z
select 
a.driver_id,b.driver_city_name,a.accdnt_score,a.conflict_score,a.sexualharass_score
from 
(select driver_id,accdnt_score,conflict_score,sexualharass_score
from safety_score.gulfstream_driver_score where year=2017 and month=7 and day=9
)a
join
(
select driver_id,driver_city_name 
from gulfstream_dw.dw_v_driver_base where concat_ws('-',year,month,day)='2017-07-09'
and driver_city_name like '%杭州%'
)b
on a.driver_id=b.driver_id;



select count(*)
from gulfstream_dw.dw_v_driver_base where concat_ws('-',year,month,day)='2017-07-09'
and driver_city_name like '%杭州%'

select * from (select* from h_z
order by accdnt_score desc) a limit 103790

select * from (select* from h_z
order by conflict_score desc) a limit 103790

select * from (select* from h_z
order by sexualharass_score desc) a limit 103790

select *,
ROW_NUMBER() OVER() AS id   from (select* from xi_an
order by sexualharass_score desc) a limit 34063


safety_score.gulfstream_driver_score，性骚扰评分:sexualharass_score字段为空；
西安司机数:681260

top 5%：34063
重大投诉评分:accdnt_score->0.006773314
车内冲突评分:conflict_score->0.0010703666
性骚扰评分:sexualharass_score->NULL

top 10%:68126
重大投诉评分:accdnt_score->0.001356836
车内冲突评分:conflict_score->0.00022175758
性骚扰评分:sexualharass_score->NULL

杭州司机数：1037908

top 5%：51895
重大投诉评分:accdnt_score->0.00644058
车内冲突评分:conflict_score->0.00063804415
性骚扰评分:sexualharass_score->NULL

top 10%:103790
重大投诉评分:accdnt_score->0.0007461662
车内冲突评分:conflict_score->0.0001144848
性骚扰评分:sexualharass_score->NULL



create table service_security.driver_xsr_score
(
driver_id bigint COMMENT'司机ID',
city_id int COMMENT'城市ID',
severeharass_score_ma float COMMENT'司机性骚扰乘客指数',
percentage float COMMENT'百分比'
)
partitioned by(
`year` string,
`month` string,
`day` string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/driver_xsr_score'


=============================获取安全分到自己库==================================
insert overwrite table service_security.driver_xsr_score
partition(year,month,day)
select
driver_id,
city_id,
severeharass_score_ma,
percentage,
'2017' as year,
'07' as month,
'09' as day
from
safety_score.gulfstream_driver_xsr_score
================================================================================
性骚扰---西安，杭州

create table service_security.xa_xsr(
driver_id BIGINT COMMENT'司机ID',
city_name STRING COMMENT'城市名',
severeharass_score_ma float COMMENT'司机性骚扰乘客指数',
percentage float COMMENT'百分比'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/xa_xsr'

=============================

insert overwrite table service_security.hz_xsr
select 
a.driver_id,b.driver_city_name,a.severeharass_score_ma
,a.percentage
from 
(select driver_id,severeharass_score_ma,percentage
from safety_score.gulfstream_driver_xsr_score where year=2017 and month=7 and day=9
)a
join
(
select driver_id,driver_city_name 
from gulfstream_dw.dw_v_driver_base where concat_ws('-',year,month,day)='2017-07-09'
and driver_city_name like '%杭州%'
)b
on a.driver_id=b.driver_id
order by a.percentage;

性骚扰safety_score.gulfstream_driver_xsr_score总数：139812
杭州-76170
西安-63641

西安-司机性骚扰乘客指数-top5-->0.001839474

select * from xa_xsr where percentage<=5;

杭州-司机性骚扰乘客指数-top5-->0.001447684

select * from hz_xsr where percentage<=5;
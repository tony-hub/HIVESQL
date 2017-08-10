create table service_security.poi_all(
poi_name STRING COMMENT'商圈名'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/poi_all'

insert overwrite table service_security.city_name
select  a.city_name,
ROW_NUMBER() OVER() AS id 
from
(
select  city_name,count(1) as cnt from normal_orders
where instr(city_name, '市')>0 or instr(city_name, '县')>0
or instr(city_name, '州')>0 or instr(city_name, '区')>0
or instr(city_name, '盟')>0
group by city_name
order by cnt 
)a


insert overwrite table service_security.poi_all
select d.name,
ROW_NUMBER() OVER() AS id 
from 
(
select  a.name,count(*) as cnt from
(select  start_catalog as name from normal_orders where concat_ws('-',year,month,day)<='2017-06-20'
union all 
select  dest_catalog as name from normal_orders where  concat_ws('-',year,month,day)<='2017-06-20'
)a
where a.name is not null
group by a.name
order by cnt
)d


dest_catalog

gulfstream_dw
brand,cnt
create table service_security.car_brand(
brand STRING COMMENT'品牌',
cnt double COMMENT'频率'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/car_brand'
driver_car_brand
gulfstream_dw.dw_v_driver_base

insert overwrite table service_security.car_brand
select
driver_car_brand,count(*)/50322467 as cnt
from gulfstream_dw.dw_v_driver_base where concat_ws('-',year,month,day)='2017-07-05'
group by driver_car_brand order by cnt desc

insert overwrite table service_security.phone_brand
select a.brand,count(*)/9928467 as cnt from
(
select lower(brand) as brand from driver_app
)a
group by a.brand order by cnt desc
insert overwrite table service_security.phone_brand
select
brand,
ROW_NUMBER() OVER() AS id 
from(
select brand,
cnt from phone_brand where brand is not null order by cnt)a

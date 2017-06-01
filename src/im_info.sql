#!/bin/bash
start=$1
spark-sql --driver-memory 8g --conf spark.driver.maxResultSize=12g --queue pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 128000000;
set hive.merge.smallfiles.avgsize=128000000;
source /home/common_plat_security/dlc/hiveset.hql;
set hive.auto.convert.join=true;
add jar /home/common_plat_security/dlc/decodeId.jar;
create temporary function dlc_decode as 'com.company.decodeId.decode';

INSERT INTO TABLE SERVICE_SECURITY.im_info
PARTITION (year,month,day)
select
im_tbl.order_id,
cic.order_status,
cic.path,
cic.content,
cic.complaint_result,
im_tbl.contents,
year,
month,
day
from
(
select
param['oids'] as order_id,concat_ws(':',param['role'],param['contents']) as contents,
param['timestamp'] as tms
From beatles_ods.imbroker
where param['prod'] in('258','260') and param['optype']='sendmsg'
and param['oids'] is not null and  concat_ws('-',year,month,day)='2015-09-01'
group by param['oids'],param['role'],param['contents'],param['timestamp']
)im_tbl
join
(
select complaint_result,order_status,content,path,order_id,year,month,day from
complaint_info_com where  concat_ws('-',year,month,day)='2015-09-01'
)cic
on cic.order_id=im_tbl.order_id
distribute by(year,month,day);"



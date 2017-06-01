#!/bin/bash
start=$1
spark-sql --driver-memory 8g --conf spark.driver.maxResultSize=12g --queue pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
source /home/common_plat_security/dlc/hiveset.hql;
set hive.auto.convert.join=true;
add jar /home/common_plat_security/dlc/decodeId.jar;
create temporary function dlc_decode as 'com.company.decodeId.decode';

INSERT INTO TABLE SERVICE_SECURITY.change_car_info
PARTITION (year,month,day)
select
im_tbl.optype,
im_tbl.order_id,
im_tbl.role,
im_tbl.prod,
im_tbl.uid,
im_tbl.peer_nick,
im_tbl.peer_uid,
im_tbl.contents,
year,
month,
day
from
(
select
param['optype'] as optype ,dlc_decode(param['oids']) as order_id,
param['role'] as role,param['prod'] as prod,param['uid'] as uid,
param['peer_nick'] as peer_nick,param['peer_uid'] as peer_uid,
param['contents']as contents From beatles_ods.imbroker
where param['prod'] in('258','260') and param['optype']='sendmsg'
and param['oids'] is not null and concat_ws('-',year,month,day)='$start'
)im_tbl
join complaint_info_com
on complaint_info_com.order_id=im_tbl.order_id
distribute by (year,month,day);"
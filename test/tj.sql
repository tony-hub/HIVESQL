===============统计的订单长度大于8的订单=============================
select count(distinct order_id),concat_ws('-',year,month,day)
 from normal_info where concat_ws('-',year,month,day)<='2017-05-03'
 and concat_ws('-',year,month,day)>='2017-04-27' and length(order_id)>8
 group by year,month,day;
 ===================单天订单长度大于8的订单===========================
 select count(distinct order_id),concat_ws('-',year,month,day)
 from normal_info where concat_ws('-',year,month,day)='2017-05-03' and length(order_id)>8
 group by year,month,day;
 ===================订单号个数统计===================================
  select  order_id,count(*) from normal_info where concat_ws('-',year,month,day)='2017-05-03'
  group by order_id limit 200;
  ==================================================================
++++++++++++++++++++统计新出的数据+++++++++++++++++++++++++++++++++++
++++++++++++++++投诉与重大投诉排除顺风车++++++++++++++++++++++++++++++++++++
 select count(*) ,concat_ws('-',year,month,day) from general_complaint_info where type=0  and product_id in(3,4,7)and order_status in (5 ,7,11 ,12)
 and order_id is not null and length(cast(order_id as string))>=6 and path not like '%顺风车s%' group by year, month, day ;
-------------------------------------------------------------------------------------------------------------------------------------------
  select count(*) ,concat_ws('-',year,month,day) from normal_info where type=0  and product_id in(3,4,7)and order_status in (5 ,7,11 ,12)
 and order_id is not null and length(cast(order_id as string))>=6  group by year, month, day ;
 -------------------------------------------------------------------------------------------------------------------------------------------
 ========================去重后对比===============================
 select count(distinct order_id) ,concat_ws('-',year,month,day) from major_complaint_info where type=0  and product_id in(3,4,7)and order_status in (5 ,7,11 ,12)
 and order_id is not null and length(cast(order_id as string))>=6 and path not like '%顺风车s%' group by year, month, day ;
 -------------------------------------------------------------------------------------------------------------------------------------------

==================POI位置分类================================
 select starting_district_name, starting_poi_type,dest_district_name,dest_poi_type from POI limit 100;

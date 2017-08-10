
select count(driver_start_distance),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13' 
and driver_start_distance=0
group by year,month,day

select count(*),count(start_dest_distance),
count(driver_start_distance),
count(forecast_time),
count(pre_total_fee),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day

start_dest_distance,
driver_start_distance, 
forecast_time, 
pre_total_fee,


select count(*),count(total_recerivetime),
count(driver_wait_time),
count(passenge_wait_time),
count(d_age),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day

total_recerivetime, 
driver_wait_time, 
passenge_wait_time,
driver_age, 


select count(*),count(d_work_times),
count(d_finish_count),
count(d_total_passenger_complaint),
count(d_total_driver_complaint),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day

driver_work_times, 
driver_finish_count, 
total_passenger_complaint,
d_total_driver_complaint,

select count(*),count(d_average_level),
count(d_star_level),
count(d_low_star_ratio),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day

driver_average_level, 
driver_star_level,
d_low_star_ratio,
d_tousulv1,

select count(*),
count(d_punish_num),
count(d_one_two_orders),
count(d_four_five_orders),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day

d_tousulv2, 
d_punish_num,
d_one_two_orders,
d_four_five_orders,


select count(*),count(d_one_star_orders),
count(d_two_star_orders),
count(d_three_star_orders),
count(d_four_star_orders),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day

d_one_star_orders,
d_two_star_orders,
d_three_star_orders,
d_four_star_orders,

select count(*),count(d_five_star_orders),
count(p_finish_count),
count(p_star_level),
count(p_finish_num1w),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day

d_five_star_orders,
passenger_finish_count, 
passenger_star_level, 
pass_finish_num1w,



gulfstream_dw.dw_v_driver_base

select count(*),
count(d_total_driver_complaint),
count(p_app_complaint_num),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13' 
group by year,month,day

p_one_two_level_num,
p_four_five_level_num,
d_total_driver_complaint,
p_app_complaint_num,

select count(*),
count(d_accdnt_score),
count(p_registered_time),
count(p_complaint_orders_all),
count(p_complaint_by_driver_all),
count(p_cancel_after_count),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day

p_cancel_rate,
d_accdnt_score,
registered_time,
p_complaint_orders_all,
p_complaint_by_driver_all,
p_cancel_after_count

select count(*),count(p_one_star_level_num),
count(p_two_star_level_num),
count(p_three_star_level_num),
count(p_four_star_level_num),
count(p_five_star_level_num),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)>='2017-06-13'
group by year,month,day

select count(*) ,count(birth_time),count(start_poi_level1),count(dest_poi_level1),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day
     
select concat_ws('-',year,month,day),count(product_id),
count(dynamic),count(combo_type), count(d_gender),count(car_level)
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day

    
select concat_ws('-',year,month,day),count(d_phone_model),
count(d_active_degree),count(d_app_type),count(d_app_num) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-13'
group by year,month,day
      
d_app_type,d_app_num,sex,passenger_age,passenger_gender

select count(*),count(d_phone_brand),count(d_phone_model),count(p_age),count(p_gender) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-18' or concat_ws('-',year,month,day)='2017-06-28' 
group by year,month,day

passenger_age,passenger_gender,passenger_id 

select count(*),count(p_phone_model),count(d_id_card_no_2),count(d_phone_1_3),
concat_ws('-',year,month,day) 
from normal_info
where concat_ws('-',year,month,day)='2017-06-18' or concat_ws('-',year,month,day)='2017-06-28' 
group by year,month,day
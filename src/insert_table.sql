INSERT INTO TABLE service_security.userinfo_auth
PARTITION(year,month,day)
select
uid,
gender,
age,
auth_state,
age_level,
year,
month,
day
from  pdw.userinfo where auth_state in(1,3,4,5,6,7) and concat_ws('-',year,month,day)='2017-04-30'
distribute by (year,month,day);
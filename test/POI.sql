set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO POI
PARTITION(year,month,day)
select * from decision.feature_extract_dinglianchao_i_124
distribute by(year,month,day);
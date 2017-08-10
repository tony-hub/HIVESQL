create table service_security.ab_orderid
(
order_id bigint COMMENT'ID'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/ab_orderid'
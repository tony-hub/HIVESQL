Create table service_security.change_car_info(
OPTYPE STRING COMMENT'操作类型',
ORDERID STRING COMMENT'订单id',
ROLE STRING COMMENT '当前用户的角色，2标识司机，1标识乘客',
PRODUCTID STRING COMMENT' 产品线id：258专车，260快车',
UID STRING COMMENT '发送方的uid',
PEER_NICK STRING COMMENT'车牌尾号',
PEER_UID STRING COMMENT'接收方的uid',
CONTENTS STRING COMMENT'聊天内容 '
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/change_car_info'

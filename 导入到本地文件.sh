hive -e "set mapred.job.queue.name=pingtaijishubu-gonggongpingtai.commonapi;
use service_security;select * from app_sensitive;">test.txt
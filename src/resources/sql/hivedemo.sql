--如果存在hive表，直接删除这个hive表。
drop table if EXISTS tb_trade_info;
--创建hive表(第一次全量，后续增量)
CREATE TABLE IF NOT EXISTS tb_trade_info (
salesmanId VARCHAR(40) comment '发展业务员Id',
salesmanName VARCHAR(20) comment '发展店铺的业务员名称',
createDate bigint comment '交易订单创建天，时间格式为yyyyMMdd的integer值，分区时间'
)
partitioned by(pt_createDate integer comment '创建天，时间格式为yyyyMMdd的integer值，分区时间')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

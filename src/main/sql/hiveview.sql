--- 交易客单价对应的视图（第一次全量，后续增量）
DROP VIEW IF EXISTS trade_info_view;
CREATE VIEW IF NOT EXISTS trade_info_view
(
shopRegTime COMMENT '商户注册时间',
levelOne COMMENT '客单价 <10元',
pt_createDate COMMENT '创建天，时间格式为yyyyMMdd的integer值，分区时间'
) COMMENT '客单价视图'
AS
select
shopRegTime,
(case when (balanceFee + payFee) < 10.0 then 1 else 0 end) as levelOne,
pt_createDate
from
tb_trade_info;

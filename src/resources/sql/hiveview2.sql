-- 广告主，流量主对应的 按照广告发布时间进行控制
DROP VIEW IF EXISTS advert_flowofmain_view;
CREATE VIEW IF NOT EXISTS advert_flowofmain_view
(
shopId COMMENT '店铺Id,主键唯一',
action COMMENT '动作类型：10，发布广告，20：流量任务（流量主）'
) COMMENT '广告主、流量主数量统计视图'
AS
select
t.shopId,
t.action
from
(
select
shopId,
action,
ROW_NUMBER() OVER(PARTITION BY advertId ORDER BY actionTime desc) AS rn
FROM
table_name
) t
where t.rn=1;

#!/usr/bin/env bash


# 假如设置为true，SparkSql会根据统计信息自动的为每个列选择压缩方式进行压缩。
spark.sql.inMemoryColumnarStorage.compressed = true
#控制列缓存的批量大小。批次大有助于改善内存使用和压缩，但是缓存数据会有OOM的风险
spark.sql.inMemoryColumnarStorage.batchSize=10000
# 广播等待超时时间，单位秒
spark.sql.broadcastTimeout=300
# 最大广播表的大小。设置为-1可以禁止该功能。当前统计信息仅支持Hive Metastore表  (10 MB)
spark.sql.autoBroadcastJoinThreshold=10485760
# 默认是200
spark.sql.shuffle.partitions=200
# 打包传入一个分区的最大字节，在读取文件的时候。  (128 MB)
spark.sql.files.maxPartitionBytes=134217728
# 用相同时间内可以扫描的数据的大小来衡量打开一个文件的开销。当将多个文件写入同一个分区的时候该参数有用。
# 该值设置大一点有好处，有小文件的分区会比大文件分区处理速度更快（优先调度）。  4194304 (4 MB)
# 这个参数就是合并小文件的阈值，小于这个阈值的文件将会合并
spark.sql.files.openCostInBytes=4194304









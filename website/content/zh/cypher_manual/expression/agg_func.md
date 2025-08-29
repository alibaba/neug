# 聚合函数

聚合函数主要用于对当前数据进行分组，并对每个组内的元素执行聚合操作，最终每个组只产生一个值。NeuG 支持的聚合函数如下：

函数 | 描述 | 可与 DISTINCT 一起使用 | 示例
---------|-------------|---------------------------|--------
count | 返回行数 | YES | Return count(a.name);
collect | 将元素收集到单个列表中 | YES | Return collect(a.name);
min | 返回最小值 | NO | Return min(a.age);
max | 返回最大值 | NO | Return max(a.age);
sum | 对值求和 | NO | Return sum(a.age);
avg | 返回平均值 | NO | Return avg(a.age);
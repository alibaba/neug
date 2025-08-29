# 图模式函数

除了前面介绍的各种基于关系数据的函数操作外，NeuG 还专门支持一组用于操作图模式生成的节点、边和路径数据的函数。

## 节点函数

函数 | 描述 | 示例
---------|-------------|--------
ID() | 获取节点/边的内部 ID | Return (a) Return ID(a)
LABEL() | 获取节点/边的标签 | Match (a) Return LABEL(a)

## 边函数

除了 ID 和 LABEL 函数外，还有以下基于边的函数操作：

函数 | 描述 | 示例
---------|-------------|--------
START_NODE() | 返回边数据的起始节点 | Match ()-[b]->() Return START_NODE(b);
END_NODE() | 返回边数据的结束节点 | Match ()-[b]->() Return END_NODE(b);

## 路径函数

函数 | 描述 | 示例
---------|-------------|--------
NODES | 返回路径中的所有节点 | Match (a)-[b*2..3]->() Return NODES(b);
RELS | 返回路径中的所有边 | Match (a)-[b*2..3]->() Return RELS(b);
PROPERTIES | 从节点/边中返回指定属性 | Match (a)-[b*2..3]->() Return PROPERTIES(nodes(b), 'name'), PROPERTIES(rels(b), 'weight');
IS_TRAIL | 检查路径是否包含重复的边（如果不包含则返回 `true`） | Match (a)-[b*2..3]->() Return IS_TRAIL(b);
IS_ACYCLIC | 检查路径是否包含重复的节点（如果不包含则返回 `true`） | Match (a)-[b*2..3]->() Return IS_ACYCLIC(b);
LENGTH | 返回路径的长度 | Match (a)-[b*2..3]->() Return LENGTH(b);
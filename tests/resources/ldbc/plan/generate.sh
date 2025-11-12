#!/bin/bash

# 要遍历搜索的目录
SEARCH_DIR="/data/flex_ldbc_snb/queries"

# 固定的配置文件路径
CONFIG_FILE="/data/flex_ldbc_snb/generated_pb/compiler_config.yaml"

# 固定的无用占位参数路径（放当前执行目录）
TEMP_PARAM="$(pwd)/temp.cypher.yaml"

COMPILER_PATH="/data/compiler.jar"

# 遍历所有 .cypher 文件
find "$SEARCH_DIR" -type f -name "*.cypher" | while read -r cypher_file; do
    # 在当前目录生成的 pb 文件名（和 cypher 文件同名不同后缀）
    base_name=$(basename "$cypher_file" .cypher)
    pb_file="$(pwd)/${base_name}.pb"

    echo "Processing: $cypher_file -> $pb_file"
    java -cp "$COMPILER_PATH" com.alibaba.graphscope.common.ir.tools.GraphPlanner \
        "$CONFIG_FILE" "$cypher_file" "$pb_file" "$TEMP_PARAM"
done

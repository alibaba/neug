#!/bin/bash
# Copyright 2020 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <image_name> <version> <beijing or hongkong> <create or update or inspect>"
    exit 0
fi

# image_name
image_name=$1
# 0.1.0
version=$2
# beijing or hongkong
region=$3
# update or create
operation=$4


if [[ "${operation}" == "create" ]]; then
    echo "Create mainfest registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version}"

    docker manifest create \
        registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version} \
        registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version}-arm64 \
        registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version}-x86_64

    docker manifest push registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version}

elif [[ "${operation}" == "update" ]]; then
    echo "Update mainfest registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version}"

    docker manifest rm registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version}
    docker manifest create \
        registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version} \
        registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version}-arm64 \
        registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version}-x86_64

    docker manifest push registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version}

elif [[ "${operation}" == "inspect" ]]; then
    docker manifest inspect registry.cn-${region}.aliyuncs.com/neug/${image_name}:${version} | jq '.manifests[] | { "arch": .platform.architecture, "digest": .digest }'
fi
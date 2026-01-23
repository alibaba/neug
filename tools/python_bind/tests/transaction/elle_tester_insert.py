#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
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
#

import random

import elle_tester as elle
import networkx as nx


# 定义读依赖，所有的read结果从少到多存储在results中，相同的结果group在ids中
# 例如results=[[], [1, 2], [1, 2, 3]], ids=[[], [1], [2, 3]]
# 表示初始状态为空，id=1的事务读到了[1, 2]，id=2,3的事务读到了[1, 2, 3]
class ReadDependency:
    results: list[list[int]]
    ids: list[list[int]]

    def __init__(self):
        self.results = []
        self.ids = []


class ElleTesterInsertEdge(elle.ElleTester):
    def __init__(self):
        super().__init__()
        self.num_of_nodes = 2
        self.num_of_query = 1
        self.num_of_trans = 100

    # 初始化数据库
    # 创建若干个Person点用于边的Read/Insert
    def init(self):
        super().init()
        elle.send(self.endpoint, "CREATE NODE TABLE Person(id INT64, PRIMARY KEY (id))")
        elle.send(
            self.endpoint, "CREATE REL TABLE Knows(FROM Person TO Person, id INT64)"
        )
        for i in range(1, self.num_of_nodes + 1):
            elle.send(self.endpoint, f"CREATE (p: Person {{id: {i}}});")
            self.track_id[i] = 0

    # 随机生成查询
    def generate_single_query(self, tot_id):
        if random.random() > 0.5:
            # 生成点操作
            if random.random() > 0.3:
                return "Insert", "Person", [tot_id]
            else:
                return "Read", "Person", []
        else:
            # 生成边操作
            start_node = random.randint(1, self.num_of_nodes)
            end_node = random.randint(1, self.num_of_nodes)
            if random.random() > 0.3:
                return "Insert", f"Knows|{start_node}|{end_node}", [tot_id]
            else:
                return "Read", f"Knows|{start_node}|{end_node}", []

    # 将read的结果排序合并
    def grouping_results(
        self,
        target: str,
        results: list[[int, elle.QueryTriplet]],
        dependency: ReadDependency,
    ):
        # Step 1: 读到最终结果
        query_str = elle.construct_query("Read", target, None)
        result = elle.send(self.endpoint, query_str)
        full_version = [row[0] for row in result]
        print("full_version:", full_version)

        # Step 2: 考虑到read的结果可能是乱序，规定read内部的顺序（以full version为标准）
        index_map = {item: i for i, item in enumerate(full_version)}
        for result in results:
            result[1].result.sort(key=lambda x: index_map[x])

        # Step  3: 根据read的数量排序
        sorted_results = sorted(results, key=lambda x: len(x[1].result))
        print("sorted_results:")
        for result in sorted_results:
            print(result)

        # Step 4: 相同的read结果合并
        dependency.results = [[]]
        dependency.ids = [[]]
        for trans_id, triplet in sorted_results:
            if len(triplet.result) == len(dependency.results[-1]):
                # len相同即说明是相同的read结果
                dependency.ids[-1].append(trans_id)
            else:
                # 否则说明是新的read结果
                dependency.results.append(triplet.result)
                dependency.ids.append([trans_id])
        if dependency.results[-1] != full_version:
            dependency.results.append(full_version)
            dependency.ids.append([])

    def connect_dependency_edges(self, dependency: ReadDependency):
        # 1. r-r边，相邻两个read结果存在先后依赖关系
        print("Step 1: r-r edge")
        for i in range(len(dependency.results) - 1):
            for id_1 in dependency.ids[i]:
                for id_2 in dependency.ids[i + 1]:
                    elle.connect(self.G, "rr", id_1, id_2)

        # 2. i-r / r-i边，相邻两个read结果的差值，表示这两个read操作中间的insert操作
        print("Step 2: i-r / r-i edge")
        for i in range(len(dependency.results) - 1):
            prev_read_result = dependency.results[i]
            succ_read_result = dependency.results[i + 1]
            for j in range(len(prev_read_result), len(succ_read_result)):
                new_x = succ_read_result[j]
                for id_1 in dependency.ids[i]:
                    elle.connect(self.G, "ri", id_1, self.track_id[new_x])
                for id_2 in dependency.ids[i + 1]:
                    elle.connect(self.G, "ir", self.track_id[new_x], id_2)

    # 实现依赖图的构造
    def build_graph(self):
        # Step 1: 将所有的read结果根据target分类
        read_results = {}
        for transaction in self.transactions.values():
            for query in transaction.queries:
                if query.operator == "Read":
                    read_results.setdefault(query.target, []).append(
                        [transaction.id, query]
                    )

        # Step 2: 对于每种target，构造依赖边
        for target, results in read_results.items():
            print("--------------------------------")
            print("Update the dependency graph for", target)
            print("--------------------------------")

            # Step 2.1: 将read的结果排序合并
            dependency = ReadDependency()
            self.grouping_results(target, results, dependency)
            print("Discretization:")
            for i in range(len(dependency.results)):
                print(dependency.ids[i], dependency.results[i])

            # Step 2.2: 构造依赖边
            self.connect_dependency_edges(dependency)

        # Step 3: 添加线性依赖
        self.connect_linearizability_edges()


def test_elle_insert_concurrent():
    # Test Read/Insert concurrent dependency detection.
    elle_tester = ElleTesterInsertEdge()
    elle_tester.start_test()

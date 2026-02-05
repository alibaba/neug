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


class ElleTesterDelete(elle.ElleTester):
    def __init__(self):
        super().__init__()
        self.num_of_nodes = 2
        self.num_of_query = 1
        self.num_of_trans = 100

    # 初始化数据库
    # 创建若干个Person点用于边的Read/Insert/Delete
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
        op = random.choices(["Read", "Insert", "Delete"], weights=[5, 3, 2], k=1)[0]
        if random.random() > 1:
            # 生成点操作
            if op == "Insert":
                return "Insert", "Person", [tot_id]
            elif op == "Delete":
                return "Delete", "Person", [tot_id]
            else:
                return "Read", "Person", []
        else:
            # 生成边操作
            start_node = random.randint(1, self.num_of_nodes)
            end_node = random.randint(1, self.num_of_nodes)
            if op == "Insert":
                return "Insert", f"Knows|{start_node}|{end_node}", [tot_id]
            elif op == "Delete":
                return "Delete", f"Knows|{start_node}|{end_node}", [tot_id]
            else:
                return "Read", f"Knows|{start_node}|{end_node}", []

    def connect_dependency_edges(self, dependency: ReadDependency):
        # 1. r-r / r-d / d-r边，相邻的read/delete之间存在依赖关系
        # 其中，r-r边是根据read结果的子集关系得出，r-d / d-r边根据delete的全局锁给出
        print("Step 1: r-r / r-d / d-r edge")
        for i in range(len(dependency.results) - 1):
            for id_1 in dependency.ids[i]:
                for id_2 in dependency.ids[i + 1]:
                    elle.connect(self.G, "rr/rd/dr", id_1, id_2)

        for i in range(len(dependency.results) - 1):
            # 枚举相邻的一对read_result
            # 其中，跨越了delete操作的两个read也需要统计
            # 保证最后一个一定是final_version的read结果，避免i+2越界的情况
            prev_read_id = i
            succ_read_id = i + 1
            if dependency.results[prev_read_id] == [None]:
                continue
            while dependency.results[succ_read_id] == [None]:
                succ_read_id += 1

            prev_read_set = set(dependency.results[prev_read_id])
            succ_read_set = set(dependency.results[succ_read_id])

            print("Step 2: i-r / r-i edge")
            print("prev_read_set:", prev_read_set, "succ_read_set:", succ_read_set)
            # 2. i-r / r-i边，相邻两个read结果中增加的那部分，表示两个read操作之间的insert
            for ins_x in succ_read_set - prev_read_set:
                for id_1 in dependency.ids[prev_read_id]:
                    elle.connect(self.G, "ri", id_1, self.track_id[ins_x])
                for id_2 in dependency.ids[succ_read_id]:
                    elle.connect(self.G, "ir", self.track_id[ins_x], id_2)

            print("Step 3: d-r / r-d edge")
            print("prev_read_set:", prev_read_set, "succ_read_set:", succ_read_set)
            # 3. d-r / r-d边，相邻两个read结果中减少的那部分，表示两个read操作之间的delete
            for del_x in prev_read_set - succ_read_set:
                for id_1 in dependency.ids[prev_read_id]:
                    elle.connect(self.G, "rd", id_1, self.track_id[del_x] + 10000)
                for id_2 in dependency.ids[succ_read_id]:
                    elle.connect(self.G, "dr", self.track_id[del_x] + 10000, id_2)

    def solve_overlap_results(self, results):
        # 解决read和delete操作的时间重叠时，可能存在的先后依赖关系
        for i in range(len(results)):
            if results[i].queries[0].operator == "Delete":
                delete_id = results[i].queries[0].result[0]
                head = []
                tail = []
                # 向后查找，直到另一个Delete操作
                j = i + 1
                while j < len(results):
                    if results[j].queries[0].operator == "Delete":
                        break
                    # 如果产生重叠并且存在delete结果，放到head中，否则放到tail中
                    if (
                        results[j].queries[0].operator == "Read"
                        and results[j].start_time < results[i].end_time
                        and delete_id in results[j].queries[0].result
                    ):
                        head.append(results[j])
                        print("SWAP:")
                        print(
                            results[i].id,
                            results[i].queries[0].operator,
                            results[i].queries[0].result,
                            results[i].start_time,
                            results[i].end_time,
                        )
                        print(
                            results[j].id,
                            results[j].queries[0].operator,
                            results[j].queries[0].result,
                            results[j].start_time,
                            results[j].end_time,
                        )
                    else:
                        tail.append(results[j])
                    j += 1
                # 将head列表-result-i-tail列表合并，放回原来的位置
                results[i:j] = head + [results[i]] + tail

    # 将read的结果排序合并
    def grouping_results(
        self,
        results: list[[int, elle.QueryTriplet]],
        dependency: ReadDependency,
    ):
        # Step  1: 根据read的数量排序
        sorted_results = sorted(results, key=lambda x: len(x[1].result))

        # Step 2: 相同的read结果合并
        for trans_id, triplet in sorted_results:
            if triplet.result == dependency.results[-1]:
                dependency.ids[-1].append(trans_id)
            else:
                # 否则说明是新的read结果
                dependency.results.append(triplet.result)
                dependency.ids.append([trans_id])

    def build_graph(self):
        # Step 1: 将所有的read和delete结果根据target分类
        read_delete_results = {}
        for transaction in self.transactions.values():
            for query in transaction.queries:
                if query.operator == "Read" or query.operator == "Delete":
                    read_delete_results.setdefault(query.target, []).append(transaction)

        # Step 2: 对于每种target，根据delete结果分块
        for target, results in read_delete_results.items():
            print("--------------------------------")
            print("Update the dependency graph for", target)
            print("--------------------------------")

            # Step 2.1: 对结果根据时间戳排序
            sorted_results = sorted(results, key=lambda t: t.end_time)
            self.solve_overlap_results(sorted_results)

            # Step 2.2: 根据delete结果分块
            dependency = ReadDependency()
            dependency.results = [[]]
            dependency.ids = [[]]
            read_block = []

            for t in sorted_results:
                if t.queries[0].operator == "Read":
                    # 遇到read，加入当前的read块
                    read_block.append([t.id, t.queries[0]])
                elif t.queries[0].operator == "Delete":
                    # 遇到delete，合并排序当前read块
                    self.grouping_results(read_block, dependency)
                    read_block = []
                    dependency.results.append([None])
                    dependency.ids.append([t.id + 10000])

            # Step 2.3: 追加最终的read结果
            query_str = elle.construct_query("Read", target, None)
            result = elle.send(self.endpoint, query_str)
            final_version = [row[0] for row in result]
            print("final_version:", final_version)
            if dependency.results[-1] != final_version:
                dependency.results.append(final_version)
                dependency.ids.append([])
            print("Discretization:")
            for i in range(len(dependency.results)):
                if dependency.results[i] == [None]:
                    print("Transaction:", dependency.ids[i], "Delete")
                else:
                    print(
                        "Transaction:",
                        dependency.ids[i],
                        "Read:",
                        dependency.results[i],
                    )

            # Step 2.4: 构造依赖边
            self.connect_dependency_edges(dependency)

            # Step 2.5: 添加线性依赖
            self.connect_linearizability_edges()


def test_elle_delete_concurrent():
    # Test Read/Insert concurrent dependency detection.
    elle_tester = ElleTesterDelete()
    elle_tester.start_test()

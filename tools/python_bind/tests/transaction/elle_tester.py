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

import os
import random
import shutil
import sys
import time
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

import threading
from datetime import datetime

import networkx as nx

from neug.database import Database
from neug.session import Session


# 定义查询三元组
# operator：操作类型，如Insert、Read
# target：操作目标，如Person、Knows|1|2，边会带上两个端点的id
# result：操作结果，如[1]/[2, 3, 4]，作为Insert操作的输入参数/Read操作的输出结果
class QueryTriplet:
    operator: str
    target: str
    result: list[int]

    def __init__(self, operator: str, target: str, result: list[int]):
        self.operator = operator
        self.target = target
        self.result = result

    def __repr__(self):
        return f"QueryTriplet({self.operator}--{self.target}--{self.result})"


# 定义transaction，包含多个query以及时间戳
# 如果是删除事务，需要额外一个temp用于提前写入需要删除的元素
class Transaction:
    id: int
    queries: list[QueryTriplet]
    start_time: datetime
    end_time: datetime

    temp_query: QueryTriplet | None
    temp_start_time: datetime
    temp_end_time: datetime

    def __init__(self, id: int, queries: list[QueryTriplet]):
        self.id = id
        self.queries = queries
        self.start_time = None
        self.end_time = None
        self.temp_query = None
        self.temp_start_time = None
        self.temp_end_time = None


# 发送单条查询，仅用于初始化
def send(endpoint, query: str):
    # print(query)
    session = Session.open(endpoint)
    result = session.execute(query)
    record = list(result)
    # print(record)
    session.close()
    return record


# 发送transaction里的所有查询
def sends(transaction: Transaction, endpoint: str):
    # 每个线程创建自己的 Connection
    session = Session.open(endpoint)
    # 随机等待，模拟并发
    time.sleep(random.random() * 0.5)

    # 根据查询三元组组装cypher查询并执行
    for query in transaction.queries:
        if query.operator == "Insert":
            query_str = construct_query(query.operator, query.target, query.result[0])

            transaction.start_time = datetime.now()
            session.execute(query_str)
            transaction.end_time = datetime.now()

        elif query.operator == "Read":
            query_str = construct_query(query.operator, query.target, None)

            transaction.start_time = datetime.now()
            result = session.execute(query_str)
            transaction.end_time = datetime.now()

            query.result = [row[0] for row in result]

        elif query.operator == "Delete":
            # 预先插入待删除的元素
            query_str = construct_query("Insert", query.target, query.result[0])
            transaction.temp_query = QueryTriplet("Insert", query.target, query.result)
            transaction.temp_start_time = datetime.now()
            session.execute(query_str)
            transaction.temp_end_time = datetime.now()

            time.sleep(random.random() * 0.1)

            query_str = construct_query("Delete", query.target, query.result[0])

            transaction.start_time = datetime.now()
            session.execute(query_str)
            transaction.end_time = datetime.now()


# 组装cypher查询
def construct_query(operator: str, target: str, id: int | None):
    query_str = ""
    if operator == "Insert":
        key = target.split("|")
        if len(key) == 1:
            query_str = f"""CREATE (n: Person {{id: {id}}})"""
        else:
            label, start_node, end_node = key
            query_str = f"""
            MATCH (n: Person {{id: {start_node}}}), (m: Person {{id: {end_node}}})
            CREATE (n)-[:Knows {{id: {id}}}]->(m)"""
    elif operator == "Read":
        key = target.split("|")
        if len(key) == 1:
            query_str = """MATCH (n: Person) RETURN n.id;"""
        else:
            label, start_node, end_node = key
            query_str = f"""
            MATCH (n: Person {{id: {start_node}}})-[k:Knows]->(m: Person {{id: {end_node}}})
            RETURN k.id;"""
    elif operator == "Delete":
        key = target.split("|")
        if len(key) == 1:
            query_str = f"""MATCH (n: Person {{id: {id}}}) DELETE n"""
        else:
            label, start_node, end_node = key
            query_str = f"""
            MATCH (n: Person {{id: {start_node}}})-[k:Knows {{id: {id}}}]->(m: Person {{id: {end_node}}}) DELETE k
            """

    return query_str


# 添加依赖边
def connect(G: nx.DiGraph, edge_type: str, from_id: int, to_id: int, log: bool = True):
    if log:
        print(from_id, "->", to_id)
    if from_id != to_id:
        G.add_edge(from_id, to_id, type=edge_type)


class ElleTester:
    def __init__(self):
        self.num_of_nodes = 2
        self.num_of_query = 1
        self.num_of_trans = 100

        self.db = None
        self.endpoint = None

        self.track_id = {}
        self.transactions: map[int, Transaction] = {}
        self.G = None
        pass

    # 初始化数据库和依赖图
    def init(self):
        db_dir = Path("./my_db")
        shutil.rmtree(db_dir, ignore_errors=True)
        self.db = Database(db_path=str(db_dir), mode="w")
        self.endpoint = self.db.serve(port=10010, host="localhost", blocking=False)
        self.G = nx.DiGraph()

    # 为每个事务生成查询，相关参数使用递增标识tot_id
    def generate(self):
        for i in range(1, self.num_of_trans + 1):
            tot_id = i * 100 + 1
            transaction = Transaction(i, [])
            for j in range(self.num_of_query):
                operator, target, result = self.generate_single_query(tot_id)
                transaction.queries.append(QueryTriplet(operator, target, result))
                self.track_id[tot_id] = i
                tot_id += 1
            self.transactions[i] = transaction

    # 创建多线程模拟并发查询
    def run(self):
        print("------ start executing ------")
        threads = []
        for i in range(1, self.num_of_trans + 1):
            # 每个线程自行连接数据库，执行transaction
            thread = threading.Thread(
                target=sends,
                args=(self.transactions[i], self.endpoint),
            )
            threads.append(thread)

        # 启动所有线程（并发执行）
        for t in threads:
            t.start()

        # 等待所有线程完成
        for t in threads:
            t.join()

    # 添加线性一致性依赖边
    def connect_linearizability_edges(self):
        print("Final Step: linearizability edge:")
        for i in range(1, self.num_of_trans):
            if self.transactions[i].queries[0].operator == "Delete":
                continue
            for j in range(i + 1, self.num_of_trans + 1):
                if self.transactions[j].queries[0].operator == "Delete":
                    continue
                if self.transactions[i].end_time < self.transactions[j].start_time:
                    connect(self.G, "li", i, j, False)
                if self.transactions[j].end_time < self.transactions[i].start_time:
                    connect(self.G, "li", j, i, False)

    # 检测依赖图是否有环
    def detect_cycle(self):
        try:
            cycle = nx.find_cycle(self.G, orientation="original")
            print("Cycle detected")
            for u, v, d in cycle:
                edge_type = self.G.edges[u, v].get("type")
                print(f"edge {u}-{v}: edge_type={edge_type}")
            assert False, "Cycle detected"
        except nx.NetworkXNoCycle:
            print("No cycle detected")

    # 打印transaction信息（id顺序）
    def print_transactions_by_id(self):
        for i in range(1, self.num_of_trans + 1):
            for query in self.transactions[i].queries:
                print(
                    "Transaction ", i, "->", query.operator, query.target, query.result
                )

    # 打印transaction信息（时间顺序）
    def print_transactions_by_time(self):
        # 收集所有开始和结束时间，合并后按时间排序
        all_events = []
        for i in range(1, self.num_of_trans + 1):
            all_events.append(
                (
                    self.transactions[i].start_time,
                    i,
                    "start",
                    self.transactions[i].queries[0],
                )
            )
            all_events.append(
                (
                    self.transactions[i].end_time,
                    i,
                    "finish",
                    self.transactions[i].queries[0],
                )
            )
            if self.transactions[i].temp_query is not None:
                all_events.append(
                    (
                        self.transactions[i].temp_start_time,
                        i,
                        "start",
                        self.transactions[i].temp_query,
                    )
                )
                all_events.append(
                    (
                        self.transactions[i].temp_end_time,
                        i,
                        "finish",
                        self.transactions[i].temp_query,
                    )
                )
        all_events.sort(key=lambda x: x[0])

        # 输出所有事件（开始和结束合并，按时间排序）
        for event_time, trans_id, event_type, q in all_events:
            print(
                "Transaction",
                trans_id,
                event_type,
                q.operator,
                q.result,
                "at:",
                event_time,
            )

    # 打印强连通分量（对应环）
    def print_sccs(self):
        sccs = nx.strongly_connected_components(self.G)
        for i, nodes in enumerate(sccs, 1):
            subgraph = self.G.subgraph(nodes)
            if subgraph.number_of_edges() > 0:
                print("----------------")
                print(f"component {i} (node: {nodes}):")
                for u, v, attrs in subgraph.edges(data=True):
                    print(f"  edge: {u} -> {v}, type: {attrs}")

    # 抽象函数，子类负责生成具体的查询
    def generate_single_query(self, tot_id):
        pass

    # 抽象函数，子类负责构造具体的依赖图
    def build_graph(self):
        pass

    # 启动测试
    def start_test(self):
        self.init()
        self.generate()
        self.run()
        self.build_graph()
        self.detect_cycle()

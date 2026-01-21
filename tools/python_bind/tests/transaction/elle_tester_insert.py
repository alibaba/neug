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


class QueryTriplet:
    operator: str
    target: str
    result: list[int]

    def __init__(self, operator: str, target: str, result: list[int]):
        self.operator = operator
        self.target = target
        self.result = result


class Transaction:
    id: int
    queries: list[QueryTriplet]
    start_time: datetime
    end_time: datetime

    def __init__(self, id: int, queries: list[QueryTriplet]):
        self.id = id
        self.queries = queries
        self.start_time = None
        self.end_time = None


# send a single query
def send(endpoint, query: str):
    print(query)
    session = Session.open(endpoint)
    result = session.execute(query)
    record = list(result)
    print(record)
    session.close()
    return record


# create a transaction to send queries
def sends(transaction: Transaction, endpoint: str):
    print("START TRANSACTION", transaction.id)
    max_retries = 100
    retry_delay = 0.1  # 100ms
    for attempt in range(max_retries):
        try:
            # 每个线程创建自己的 Connection
            session = Session.open(endpoint)
            time.sleep(random.random() * 0.2)
            transaction.start_time = datetime.now()
            for query in transaction.queries:
                if query.operator == "Insert":
                    query_str = f"Create (n: Person {{id: {query.result[0]}}})"
                    session.execute(query_str)
                    print(
                        "Transaction ID: ",
                        transaction.id,
                        query.operator,
                        query.target,
                        query.result,
                    )
                elif query.operator == "Read":
                    query_str = "MATCH (n:Person) RETURN n.id;"
                    result = session.execute(query_str)
                    time.sleep(0.1)
                    query.result = [row[0] for row in result]
                    print(
                        "Transaction ID:",
                        transaction.id,
                        query.operator,
                        query.target,
                        query.result,
                    )
            # 提交事务
            transaction.end_time = datetime.now()
            return  # 成功执行，退出重试循环
        except RuntimeError as e:
            error_msg = str(e)
            if (
                "Only one write transaction at a time is allowed" in error_msg
                or "Cannot start a new write transaction" in error_msg
            ):
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise  # 重试次数用完，抛出异常
            else:
                raise  # 其他错误直接抛出


# connect dependency edges
def connect(G: nx.DiGraph, edge_type: str, from_id: int, to_id: int):
    print(from_id, "->", to_id)
    if from_id != to_id:
        G.add_edge(from_id, to_id, type=edge_type)


class ElleTester:
    def __init__(self):
        self.num_of_nodes = 1
        self.num_of_query = 1
        self.num_of_trans = 10
        self.db = None
        self.endpoint = None
        self.track_id = {}
        self.transactions: map[int, Transaction] = {}
        self.lock = threading.Lock()  # 用于保护共享数据的锁
        self.transaction_time = None
        self.read_dependency_results = None
        self.read_dependency_ids = None
        pass

    # generate a single query
    def generate_single_query(self, tot_id) -> QueryTriplet:
        if random.random() > 0.3:
            return QueryTriplet("Insert", "Person", [tot_id])
        else:
            return QueryTriplet("Read", "Person", [])

    # initialization
    def init(self):
        db_dir = Path("./my_db")
        shutil.rmtree(db_dir, ignore_errors=True)
        self.db = Database(db_path=str(db_dir), mode="w")
        self.endpoint = self.db.serve(port=10010, host="localhost", blocking=False)
        send(self.endpoint, "CREATE NODE TABLE Person(id INT64, PRIMARY KEY (id))")
        send(self.endpoint, "MATCH (n:Person) DELETE n;")

    # generate a query list for each transaction
    def generate(self):
        for i in range(1, self.num_of_trans + 1):
            tot_id = i * 100 + 1
            transaction = Transaction(i, [])
            for j in range(self.num_of_query):
                query = self.generate_single_query(tot_id)
                transaction.queries.append(query)
                self.track_id[tot_id] = i
                tot_id += 1
            self.transactions[i] = transaction

    # create multiple threads to run transactions concurrently
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

        for i in range(1, self.num_of_trans + 1):
            for query in self.transactions[i].queries:
                print(
                    "Transaction ", i, "->", query.operator, query.target, query.result
                )
        # 收集所有开始和结束时间，合并后按时间排序
        all_events = []
        for i in range(1, self.num_of_trans + 1):
            if self.transactions[i].start_time is not None:
                all_events.append((self.transactions[i].start_time, i, "start"))
            if self.transactions[i].end_time is not None:
                all_events.append((self.transactions[i].end_time, i, "finish"))

        # 按时间排序
        all_events.sort(key=lambda x: x[0])

        # 输出所有事件（开始和结束合并，按时间排序）
        for event_time, trans_id, event_type in all_events:
            if event_type == "start":
                print("Transaction", trans_id, "start at:", event_time)
            else:
                print("Transaction", trans_id, "finish at:", event_time)

    def prepare_result(self):
        read_results = []
        for i in range(1, self.num_of_trans + 1):
            for query in self.transactions[i].queries:
                if query.operator == "Read":
                    read_results.append([i, query])

        result = send(self.endpoint, "MATCH (n:Person) RETURN n.id;")
        full_version = [row[0] for row in result]

        print("full_version:", full_version)
        index_map = {item: i for i, item in enumerate(full_version)}
        for read_result in read_results:
            read_result[1].result.sort(
                key=lambda x: index_map[x]
            )  # Sort the result list in place
            print(read_result)

        # Discretization
        # Sort query_queue by the size of the read result list
        sorted_read_results = sorted(read_results, key=lambda x: len(x[1].result))
        print("sorted_read_results:", sorted_read_results)

        # save all read results (unique, initially empty)
        self.read_dependency_results = [[]]
        self.read_dependency_ids = [[]]
        for trans_id, triplet in sorted_read_results:
            if triplet.result == self.read_dependency_results[-1]:
                # same read result
                self.read_dependency_ids[-1].append(trans_id)
            else:
                # new read result
                self.read_dependency_results.append(triplet.result)
                self.read_dependency_ids.append([trans_id])
        if self.read_dependency_results[-1] != full_version:
            self.read_dependency_results.append(full_version)
            self.read_dependency_ids.append([])

        print("Discretization:")
        for i in range(len(self.read_dependency_results)):
            print(self.read_dependency_results[i], self.read_dependency_ids[i])

    # build a dependency graph
    def build_graph(self):
        G = nx.DiGraph()

        # 1. r-r edge
        print("Step 1: r-r edge")
        for i in range(len(self.read_dependency_ids) - 1):
            for id_1 in self.read_dependency_ids[i]:
                for id_2 in self.read_dependency_ids[i + 1]:
                    connect(G, "rr", id_1, id_2)

        # 2. i-r / r-i edge
        print("Step 2: i-r / r-i edge")
        for i in range(len(self.read_dependency_results) - 1):
            prev_read_result = self.read_dependency_results[i]
            succ_read_result = self.read_dependency_results[i + 1]
            for j in range(len(prev_read_result), len(succ_read_result)):
                new_x = succ_read_result[j]
                for id_1 in self.read_dependency_ids[i]:
                    connect(G, "ri", id_1, self.track_id[new_x])
                for id_2 in self.read_dependency_ids[i + 1]:
                    connect(G, "ir", self.track_id[new_x], id_2)

        # 3. Linearizability
        print("Step 3: linearizability edge:")
        for i in range(1, self.num_of_trans):
            for j in range(i + 1, self.num_of_trans + 1):
                if self.transactions[i].end_time < self.transactions[j].start_time:
                    print(i, "->", j)
                    connect(G, "li", i, j)
                if self.transactions[j].end_time < self.transactions[i].start_time:
                    print(j, "->", i)
                    connect(G, "li", j, i)

        try:
            cycle = nx.find_cycle(G, orientation="original")
            assert False, "Cycle detected"
            print("Cycle detected")
            for u, v, d in cycle:
                edge_type = G.edges[u, v].get("type")
                print(f"edge {u}-{v}: edge_type={edge_type}")
        except nx.NetworkXNoCycle:
            print("No cycle detected")

        # sccs = nx.strongly_connected_components(G)
        # for i, nodes in enumerate(sccs, 1):
        #     subgraph = G.subgraph(nodes)
        #     if subgraph.number_of_edges() > 0:
        #         print("----------------")
        #         print(f"component {i} (node: {nodes}):")
        #         for u, v, attrs in subgraph.edges(data=True):
        #             print(f"  edge: {u} -> {v}, type: {attrs}")


def test_elle_insert_concurrent():
    # Test Read/Insert concurrent dependency detection.
    elle_tester = ElleTester()
    elle_tester.init()
    elle_tester.generate()
    elle_tester.run()
    elle_tester.prepare_result()
    elle_tester.build_graph()

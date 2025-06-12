import pytest
from utils.utils import preprocess_query
from base_test import BaseTest


class TestCypher(BaseTest):

    def prepare_query(self, query_object):
        return preprocess_query(query_object.query)

    @pytest.mark.cypher_test
    def test_queries(self, neo4j_client, query_object):
        self.run_test(neo4j_client, query_object)

    @pytest.mark.cypher_benchmark
    def test_benchmark_queries(self, request, benchmark, neo4j_client, query_object):
        self.run_benchmark(neo4j_client, query_object, request, benchmark)

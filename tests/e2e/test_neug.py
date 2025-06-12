import pytest
from utils.utils import preprocess_query
from base_test import BaseTest


class TestNeug(BaseTest):

    def prepare_query(self, query_object):
        return preprocess_query(query_object.query)

    @pytest.mark.neug_test
    def test_queries(self, neug_conn, query_object):
        self.run_test(neug_conn, query_object)
        
    @pytest.mark.neug_benchmark
    def test_benchmark_queries(self, request, benchmark, neug_conn, query_object):
        self.run_benchmark(neug_conn, query_object, request, benchmark)

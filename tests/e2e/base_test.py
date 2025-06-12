import pytest
from utils.utils import (
    Query, validate_result
)

class BaseTest:
    def run_test(self, connection, query_object: Query):
        query_name = query_object.name
        query = self.prepare_query(query_object)  
        expected_output = query_object.expected_result
        check_order = query_object.check_order
        result = connection.execute(query)
        result_str = self.process_results(result)
        validate_result(query_name, result_str, expected_output, check_order)
        
    def run_benchmark(self, connection, query_object, request, benchmark):
        iterations = request.config.getoption("iterations")
        rounds = request.config.getoption("rounds")
        warmup_rounds = request.config.getoption("warmup_rounds")

        query = self.prepare_query(query_object)  
        # Benchmark the query
        benchmark.pedantic(
            connection.execute,
            args=(query,),
            iterations=iterations,
            rounds=rounds,
            warmup_rounds=warmup_rounds,
        )
        
    def prepare_query(self, query_object):
        """ prepare the query for execution."""
        return query_object.query

    def process_results(self, result):
        """Process the results for validation."""
        return [list(map(str, record)) for record in result]  
    

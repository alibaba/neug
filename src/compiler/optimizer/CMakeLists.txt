add_library(neug_optimizer
        OBJECT
        acc_hash_join_optimizer.cpp
        agg_key_dependency_optimizer.cpp
        cardinality_updater.cpp
        correlated_subquery_unnest_solver.cpp
        factorization_rewriter.cpp
        filter_push_down_optimizer.cpp
        logical_operator_collector.cpp
        logical_operator_visitor.cpp
        optimizer.cpp
        projection_push_down_optimizer.cpp
        schema_populator.cpp
        remove_factorization_rewriter.cpp
        remove_unnecessary_join_optimizer.cpp
        top_k_optimizer.cpp
        limit_push_down_optimizer.cpp
	expand_getv_fusion.cpp
	filter_push_down_pattern.cpp
        flat_join_to_expand_optimizer.cpp
        union_alias_map_optimizer.cpp
        remove_subquery_as_join.cpp
        common_pattern_reuse_optimizer.cpp
        project_join_condition_optimizer.cpp)
add_dependencies(neug_optimizer neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_optimizer>
        PARENT_SCOPE)

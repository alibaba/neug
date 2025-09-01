/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef NEUG_H_
#define NEUG_H_

/**
 * @file neug.h
 * @brief Main header file for NeuG - Hybrid Transactional/Analytical Processing
 * (HTAP) graph database
 *
 *
 * Include this header to access the main NeuG functionality for embedded graph
 * analytics and real-time transaction processing.
 *
 * @version 0.1
 * @author NeuG Development Team
 */

// Version information - must be included first
#include "neug/version.h"

// Build configuration
#include "neug/config.h"

// Core API - Main database and connection interfaces
#include "neug/main/connection.h"
#include "neug/main/file_lock.h"
#include "neug/main/neug_db.h"
#include "neug/main/query_processor.h"
#include "neug/main/query_result.h"

// Graph Database Engine - Core database functionality
#include "neug/engines/graph_db/database/graph_db.h"
#include "neug/engines/graph_db/database/graph_db_session.h"
#include "neug/engines/graph_db/database/insert_transaction.h"
#include "neug/engines/graph_db/database/read_transaction.h"
#include "neug/engines/graph_db/database/update_transaction.h"

// Graph Service - Service layer interfaces
#include "neug/engines/graph_db_service.h"

// Query Planning - Graph query optimization
#include "neug/planner/dummy_graph_planner.h"
#include "neug/planner/gopt_planner.h"
#include "neug/planner/graph_planner.h"

// Storage and Schema - Data storage abstractions
#include "neug/storages/graph_storage.h"

// Utilities - Common utilities and result types
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"

#endif  // NEUG_H_

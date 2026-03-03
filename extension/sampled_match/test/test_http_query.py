#!/usr/bin/env python3
"""
Test script for NeuG HTTP queries.

Usage:
    python test_http_query.py [--host HOST] [--port PORT] [--pattern PATTERN_FILE]
    
Examples:
    # Basic query
    python test_http_query.py
    
    # Custom server
    python test_http_query.py --host 192.168.1.100 --port 8080
    
    # Custom pattern
    python test_http_query.py --pattern /path/to/my_pattern.json
"""

import argparse
import json
import requests
import sys
import os

def send_cypher_query(host, port, query, access_mode="read"):
    """Send a Cypher query to NeuG server."""
    url = f"http://{host}:{port}/cypher"
    data = {
        "query": query,
        "access_mode": access_mode
    }
    
    print(f"Sending query to: {url}")
    print(f"Query: {query}")
    print(f"Access mode: {access_mode}")
    print("-" * 60)
    
    try:
        response = requests.post(url, json=data, timeout=300)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        print(f"ERROR: Cannot connect to server at {url}")
        print("Make sure the server is running:")
        print(f"  ./run_test.sh --server")
        return None
    except requests.exceptions.Timeout:
        print("ERROR: Request timed out")
        return None
    except Exception as e:
        print(f"ERROR: {e}")
        return None

def test_schema(host, port):
    """Get schema from server."""
    url = f"http://{host}:{port}/schema"
    
    print("Getting schema...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"ERROR getting schema: {e}")
        return None

def test_status(host, port):
    """Get server status."""
    url = f"http://{host}:{port}/status"
    
    print("Getting status...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Note: Status endpoint returned: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="NeuG HTTP Query Test")
    parser.add_argument("--host", default="localhost", help="Server host")
    parser.add_argument("--port", type=int, default=10000, help="Server port")
    parser.add_argument("--pattern", help="Pattern file path (absolute path)")
    args = parser.parse_args()
    
    # Default pattern file path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, "..", "..", "..")
    default_pattern = os.path.join(project_root, "sampling", "pattern", "pattern_with_constraints.json")
    pattern_file = args.pattern or os.path.abspath(default_pattern)
    
    print("=" * 60)
    print("  NeuG HTTP Query Test")
    print("=" * 60)
    print(f"Server: {args.host}:{args.port}")
    print(f"Pattern file: {pattern_file}")
    print("")
    
    # Test 1: Check schema
    print("=" * 60)
    print("Test 1: Get Schema")
    print("=" * 60)
    schema = test_schema(args.host, args.port)
    if schema:
        print("Schema retrieved successfully:")
        print(json.dumps(schema, indent=2)[:500] + "...")
    print("")
    
    # Test 2: Simple query
    print("=" * 60)
    print("Test 2: Simple Count Query")
    print("=" * 60)
    result = send_cypher_query(
        args.host, args.port,
        "MATCH (n) RETURN count(n) AS total_vertices"
    )
    if result:
        print("Result:")
        print(json.dumps(result, indent=2))
    print("")
    
    # Test 3: SAMPLED_MATCH query
    print("=" * 60)
    print("Test 3: SAMPLED_MATCH Query")
    print("=" * 60)
    
    if not os.path.exists(pattern_file):
        print(f"ERROR: Pattern file not found: {pattern_file}")
        return 1
    
    # Note: The pattern file path must be absolute for the server to find it
    result = send_cypher_query(
        args.host, args.port,
        f'CALL SAMPLED_MATCH("{pattern_file}")'
    )
    if result:
        print("Result:")
        print(json.dumps(result, indent=2))
    print("")
    
    print("=" * 60)
    print("Test completed!")
    print("=" * 60)
    return 0

if __name__ == "__main__":
    sys.exit(main())

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

import json
import logging
import os
from pathlib import Path

from flask import Flask
from flask import jsonify
from flask import request
from flask import send_from_directory
from flask_cors import CORS

from neug.database import Database
from neug.format import parse_and_format_results
from neug.session import Session

logger = logging.getLogger("neug")


class NeugWebUI:
    def __init__(self, db_dir=None, host="127.0.0.1", port=5000):
        self.app = Flask(__name__)
        CORS(self.app)  # Enable CORS for all routes

        self.db_dir = db_dir
        self.host = host
        self.port = port
        self.database = None
        self.session = None

        # Setup routes
        self._setup_routes()

        # Initialize database connection if db_dir is provided
        self._init_database()

    def _setup_routes(self):
        """Setup Flask routes"""

        @self.app.route("/")
        def index():
            """Serve the main index.html page"""
            resources_path = Path(__file__).parent / "resources"
            return send_from_directory(resources_path, "index.html")

        @self.app.route("/schema", methods=["GET"])
        def get_schema():
            return self.session.get_schema()

        # post cypherv2 with string
        @self.app.route("/cypherv2", methods=["POST"])
        def execute_query():
            # Get raw string from request body
            query = request.get_data(as_text=True)
            logger.info(f"Received query: {query}")
            result = self.session.execute(query)
            return jsonify({"result": result.to_dict()})

    def _init_database(self):
        """Initialize database connection"""
        self.database = Database(db_path=self.db_dir, mode="w")
        endpoint = self.database.serve(port=self.port + 1, host=self.host)
        logger.info(f"Database server started at {endpoint}")
        self.session = Session(endpoint=endpoint)

    def run(self, debug=False):
        """Start the Flask development server"""
        logger.info(f"Starting Neug Web UI on http://{self.host}:{self.port}")
        if self.db_dir:
            logger.info(f"Connected to database: {self.db_dir}")
        else:
            logger.info(
                "No database connected. Use the web interface to connect to a database."
            )

        self.app.run(host=self.host, port=self.port, debug=debug)


def start_web_ui(db_dir=None, host="127.0.0.1", port=5000, debug=False):
    """Start the Neug Web UI"""
    web_ui = NeugWebUI(db_dir=db_dir, host=host, port=port)
    web_ui.run(debug=debug)


if __name__ == "__main__":
    start_web_ui()

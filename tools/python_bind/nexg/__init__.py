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
import sys
import platform

import logging

logger = logging.getLogger("nexg")

def config_logging(log_level):
    """Set log level basic on config.
    Args:
        log_level (str): Log level of stdout handler
    """
    logging.basicConfig(level=logging.CRITICAL)

    # `NOTSET` is special as it doesn't show log in Python
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper())
    if log_level == logging.NOTSET:
        log_level = logging.DEBUG

    logger = logging.getLogger("nexg")
    logger.setLevel(log_level)

def get_build_lib_dir() -> str:
    """
    Get the build lib directory for the current development environment.
    The path is {CUR_DIR}/../build/lib.{OS}-{ARCH}-{PYTHON_VERSION}
    #OS is the operating system name (e.g., 'linux', 'darwin', 'win32')
    #ARCH is the architecture of the machine (e.g., 'x86_64', 'arm64')
    #PYTHON_VERSION is the version of Python (e.g., '3.8')
    Returns:
        str: The build lib directory.
    """
    cur_dir = os.path.dirname(__file__)
    os_name = platform.system().lower()
    build_dir = os.path.join(
        cur_dir,
        "..",
        "build",
        f"lib.{os_name}-{os.uname().machine}-{sys.version_info.major}.{sys.version_info.minor}",
        )
    logger.info("Build directory: %s", build_dir)
    if os.path.exists(build_dir):
        logger.info("Using build directory: %s", build_dir)
    return build_dir
    

config_logging("INFO")
try:
    # Try to first include the c++ extension directory, if it exists
    # it means we are in development mode.
    build_dir = get_build_lib_dir()
    if os.path.exists(build_dir):
        import sys

        sys.path.append(build_dir)
    import nexg_py_bind


except ImportError:
    raise ImportError(
        "NexG is not installed. Please install it using pip or build it from source."
    )

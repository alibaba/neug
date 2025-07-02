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
import logging
import os
import platform
import sys

logger = logging.getLogger("neug")


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

    logger = logging.getLogger("neug")
    logger.setLevel(log_level)


def get_build_lib_dir() -> str:
    """
    Get the build lib directory for the current development environment.
    The path is {CUR_DIR}/../build/lib.{OS}-{ARCH}-{PYTHON_VERSION}
    #OS is the operating system name (e.g., 'linux', 'macosx-version', 'win32')
    #ARCH is the architecture of the machine (e.g., 'x86_64', 'arm64')
    #PYTHON_VERSION is the version of Python (e.g., '3.8')
    Returns:
        str: The build lib directory.
    """
    cur_dir = os.path.dirname(__file__)
    os_name = platform.system().lower()
    build_dir = None
    if os_name == "darwin":
        # find the directory start with lib.macosx-* under build
        # and get the first one
        build_dir_parent = os.path.join(cur_dir, "..", "build")
        if os.path.exists(build_dir_parent):
            build_dirs = [
                d for d in os.listdir(build_dir_parent) if d.startswith("lib.macosx-")
            ]
            if build_dirs:
                # select the most matching directory, the directories are like lib.macosx-11.0-arm64-cpython-312
                # we should get the one matches current python version
                logger.info("Found build directories: %s", build_dirs)
                build_dirs.sort(
                    key=lambda x: (
                        x.split("-")[-1],  # e.g., cpython-312
                        x.split("-")[2],  # e.g., arm64
                    ),
                    reverse=True,
                )
                first = build_dirs[0]
                logger.info("Selected build directory: %s", first)
                if (
                    first.find(
                        f"cpython-{sys.version_info.major}{sys.version_info.minor}"
                    )
                    != -1
                    and first.find(os.uname().machine) != -1
                ):
                    build_dir = os.path.join(build_dir_parent, first)
            else:
                build_dir = None
        else:
            build_dir = None
    else:
        if sys.version_info.major == 3 and sys.version_info.minor >= 12:
            # For Python 3.8 and above, we use the new naming convention
            build_dir = os.path.join(
                cur_dir,
                "..",
                "build",
                f"lib.{os_name}-{os.uname().machine}-cpython-{sys.version_info.major}{sys.version_info.minor}",
            )
        else:
            build_dir = os.path.join(
                cur_dir,
                "..",
                "build",
                f"lib.{os_name}-{os.uname().machine}-{sys.version_info.major}.{sys.version_info.minor}",
            )
    logger.info("Build directory: %s", build_dir)
    if build_dir is not None:
        if os.path.exists(build_dir):
            logger.debug("Using build directory: %s", build_dir)
    return build_dir


config_logging("INFO")
try:
    # Try to first include the c++ extension directory, if it exists
    # it means we are in development mode.
    build_dir = get_build_lib_dir()
    if build_dir and os.path.exists(build_dir):
        import sys

        sys.path.append(build_dir)
    import neug_py_bind


except ImportError:
    raise ImportError(
        "NeuG is not installed. Please install it using pip or build it from source."
    )

from neug.async_connection import AsyncConnection
from neug.connection import Connection
from neug.database import Database
from neug.query_result import QueryResult

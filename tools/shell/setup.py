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

from setuptools import find_packages
from setuptools import setup

setup(
    name="ngcli",
    version="0.1.0",
    description="NeuG CLI Tool",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "click>=8.2.1",
        "tabulate>=0.9.0",
        "PyYAML>=6.0.2",
        "neug==0.1.0",
        "setuptools>=59.6.0",
    ],
    entry_points="""
        [console_scripts]
        ngcli=ngcli.neug_cli:cli
    """,
)

#!/bin/bash
# Copyright 2020 Alibaba Group Holding Limited.
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

if [ $# -ne 2 ]; then
    echo "Usage: $0 <LIBRARY_PATH> <DST_DIR>"
    exit 1
fi

LIBRARY_PATH="$1"
DST_DIR="$2"

if [ ! -d "$DST_DIR" ]; then
    echo "Destination directory $DST_DIR does not exist."
    exit 1
fi

if [ ! -f "$LIBRARY_PATH" ]; then
    echo "Library file $LIBRARY_PATH does not exist."
    exit 1
fi

install_name_tool=$( command -v install_name_tool )

# Extact the dependencies from the library
DEPENDENCIES=$(otool -L "$LIBRARY_PATH" | awk '{print $1}' | tail -n +2)
for DEP in $DEPENDENCIES; do
    echo "Processing dependency: $DEP"
    # if DEP start with @rpath
    if [[ "$DEP" == @rpath/* ]]; then
        # if DEP contains arrow,boost,mimalloc, then replace @rpath with the destination directory
        if [[ "$DEP" == *arrow* || "$DEP" == *boost* || "$DEP" == *mimalloc* || "$DEP" == *yaml-cpp* || "$DEP" == *gflag*  || "$DEP" == *glog* ]]; then
            new_dep="${DEP/@rpath/$DST_DIR}"
        fi
    fi
    cmd="${install_name_tool} -change $DEP $new_dep $LIBRARY_PATH"
    echo "Running command: $cmd"
    eval "$cmd"
done

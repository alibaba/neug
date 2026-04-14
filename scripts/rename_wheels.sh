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

# Default inputs; can be overridden via CLI flags
DIR=""
REVERSION=""

usage() {
  echo "Usage: $0 [-d wheel_dir] [-r reversion]"
  exit 1
}

while getopts ":d:r:h" opt; do
  case "$opt" in
    d)
      DIR="$OPTARG"
      ;;
    r)
      REVERSION="$OPTARG"
      ;;
    h)
      usage
      exit 0
      ;;
    *)
      usage
      ;;
  esac
done

if [ -z "$DIR" ] || [ -z "$REVERSION" ]; then
  usage
fi

shift $((OPTIND - 1))

# Navigate to the specified directory
cd "$DIR" || exit

# Loop through each wheel file in the directory
for file in *.whl; do
  # Use pattern matching to insert '-2-' after version number
  # new_name=$(echo "$file" | sed 's/\(neug-[0-9]\+\.[0-9]\+\.[0-9]\+\)-/\1-'"$REVERSION"'-/')
  # on macos, the file name is like neug-0.1.1-cp310-cp310-macosx_11_0_arm64.whl
  new_name=$(echo "$file" | sed -E 's/(neug-[0-9]+\.[0-9]+\.[0-9]+)-/\1-'"$REVERSION"'-/')

  # Rename the file
  mv "$file" "$new_name"
  echo $new_name
done

echo "Renaming complete."

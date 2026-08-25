#!/bin/bash

FILES=$(git diff HEAD --name-only --diff-filter=ACM | grep -E '\.(cpp|cc|c|h|hpp)$')

if [ -z "$FILES" ]; then
  exit 0
fi

echo "Running clang-format on changed files..."

for file in $FILES; do
  clang-format -i "$file"
  echo "$file" has been formated
done

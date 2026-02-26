---
name: generate-testcase
description: Analyze new commits and generate related test cases.
disable-model-invocation: true
---
## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Preparation

1. Find all commits after this branch is forked.
2. Compare two commits and extract all new files.
3. Locate the test directory. The test file should be saved in this directory.
4. Locate the spec directory, often `./specs/`.

## Generation

1. Think about some user scenarios (From basic usage with default setting, to different formats and parameters). These scenarios MUST related to the new feature.
2. Ask users to choose. Wait for users response.
3. For each user scenario, create a test function to confirm.
4. Build and run test cases. Fix problems until all test passed.

## Test

1. Check the installation test coverage packages, including `fastcov` and `lcov`.
2. Build and run test cases. Note to enable `ENABLE_GCOV` and `BUILD_TEST`.
3. Fix problems until all test passed.
4. Report and analyze the coverage result. Highlight files that the function coverage rate is not 100%.

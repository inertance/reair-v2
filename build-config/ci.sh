#!/bin/bash
set -evx

# Store the build output here so we can analyze it
build_output_dir="${TRAVIS_BUILD_DIR}/build_output.txt"

# Run gradle
${TRAVIS_BUILD_DIR}/gradlew check 2>&1 | tee "${build_output_dir}"
test ${PIPESTATUS[0]} -eq 0

# Fail the build if there are checkstyle warnings
# TODO We shouldn't need this dedicated logic to parse for checkstyle warnings and fail the build
# when they occur, but this is the best we can do until https://issues.gradle.org/browse/GRADLE-2888
# is resolved.
num_checkstyle_warnings=$( \
  grep "\[ant:checkstyle\] .*: warning:" "${build_output_dir}" \
    | wc -l \
    | awk '{print $1}' \
)

if [[ $num_checkstyle_warnings -gt 0 ]]; then
  echo "ERROR: ${num_checkstyle_warnings} checkstyle warnings present, please fix them."
  exit 1
fi

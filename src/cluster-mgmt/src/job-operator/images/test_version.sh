#!/usr/bin/env bash

assert_not_empty() {
  if [ -z "$1" ]; then
    echo "Assertion failed: Environment variable '$1' is empty or not set."
    exit 1
  fi
}

assert_equal() {
  if [[ "$1" != "$2" ]]; then
    echo "Assertion failed: Values are not equal: '$1' and '$2'"
    exit 1
  fi
}

assert_not_equal() {
  if [[ "$1" == "$2" ]]; then
    echo "Assertion failed: Values are equal: '$1' and '$2'"
    exit 1
  fi
}

TEMP_DIR=$(mktemp -d -t test-XXXXXX)
trap 'rm -rf $TEMP_DIR' EXIT

# assert version creation success
echo "test case 0 start"
touch "$TEMP_DIR"/Dockerfile
bash version.sh "$TEMP_DIR" test Dockerfile
hash1=$(cat "$TEMP_DIR/_version_test")
echo "$hash1"
assert_not_empty "$hash1"
echo "test case 0 passed"

# assert version change after file content change
echo "test case 1 start"
echo "123" >>"$TEMP_DIR"/Dockerfile
bash version.sh "$TEMP_DIR" test Dockerfile
hash2=$(cat "$TEMP_DIR/_version_test")
echo "$hash2"
assert_not_equal "$hash1" "$hash2"
echo "test case 1 passed"

# assert version keep same if content same
echo "test case 2 start"
rm -f "$TEMP_DIR"/Dockerfile
touch "$TEMP_DIR"/Dockerfile
bash version.sh "$TEMP_DIR" test Dockerfile
hash3=$(cat "$TEMP_DIR/_version_test")
echo "$hash3"
assert_equal "$hash1" "$hash3"
echo "test case 2 passed"

# assert version change if files added
echo "test case 3 start"
touch "$TEMP_DIR"/test.sh
bash version.sh "$TEMP_DIR" test Dockerfile test.sh
hash4=$(cat "$TEMP_DIR/_version_test")
echo "$hash4"
assert_not_equal "$hash3" "$hash4"
echo "test case 3 passed"

# assert version change if files added under folder
echo "test case 4 start"
mkdir "$TEMP_DIR"/scripts
touch "$TEMP_DIR"/scripts/test1.py
touch "$TEMP_DIR"/scripts/test2.py
bash version.sh "$TEMP_DIR" test Dockerfile test.sh "*scripts/*"
hash5=$(cat "$TEMP_DIR/_version_test")
echo "$hash5"
assert_not_equal "$hash4" "$hash5"
echo "test case 4 passed"

# assert version keep same with different order of filters
echo "test case 5 start"
rm -rf "$TEMP_DIR"/scripts
bash version.sh "$TEMP_DIR" test test.sh "*scripts/*" Dockerfile
hash6=$(cat "$TEMP_DIR/_version_test")
echo "$hash6"
assert_equal "$hash4" "$hash6"
echo "test case 5 passed"

echo "All tests passed!"

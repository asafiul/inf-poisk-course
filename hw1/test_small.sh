#!/bin/bash

echo "LSM Tree Test Script (Small Sizes)"
echo "=================================="

echo "Cleaning previous build..."
rm -rf build_test

echo -e "\nBuilding test version..."
mkdir -p build_test
cd build_test
cmake .. -DDEBUG=OFF -DTEST_SMALL_SIZE=ON
make -j4

echo -e "\nRunning tests..."
./test_lsm_tree

if [ $? -eq 0 ]; then
    echo -e "\n All tests passed!"
else
    echo -e "\n Some tests failed!"
fi

cd ..
echo -e "\nTest script completed."

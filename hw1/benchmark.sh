#!/bin/bash

echo "LSM Tree Benchmark Runner"

NUM_OPS=${1:-1000000}
SEED=${2:-42}
MAX_KEY=${3:-100000}
OUTPUT_FILE=${4:-benchmark_results.csv}

echo "Building benchmark version..."
echo "Parameters: ops=$NUM_OPS, seed=$SEED, max_key=$MAX_KEY, output=$OUTPUT_FILE"

rm -rf build_bench
mkdir -p build_bench
cd build_bench
cmake .. -DDEBUG=OFF -DTEST_SMALL_SIZE=ON
make -j4

echo -e "\nRunning benchmark with $NUM_OPS operations..."

./lsm_tree --bench-random $NUM_OPS $SEED $MAX_KEY $OUTPUT_FILE

echo -e "\nBenchmark completed!"
echo "CSV results saved to: build_bench/$OUTPUT_FILE"

echo -e "\nFile info:"
ls -la $OUTPUT_FILE
wc -l $OUTPUT_FILE

echo -e "\nFirst 10 lines of CSV results:"
head -10 $OUTPUT_FILE

echo -e "\nAnalyzing benchmark results..."
cd ..

if command -v python3 &> /dev/null; then
    echo "Running Python analyzer..."
    python3 benchmark_analyzer.py build_bench/$OUTPUT_FILE
    echo -e "\nAnalysis completed! Check benchmark_plots/ directory for results."
else
    echo "Python3 not found, skipping analysis"
fi

#include "lsm_tree.h"
#include "utils.h"
#include <iostream>
#include <chrono>
#include <filesystem>
#include <random>

class Benchmark
{
private:
    LSMTree &lsm;

public:
    Benchmark(LSMTree &tree) : lsm(tree) {}

    void bench_insert(int num_ops)
    {
        LOG_INFO("Running insert benchmark for %d operations...", num_ops);

        auto start = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < num_ops; i++)
        {
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i) + "_" + std::string(100, 'x');
            lsm.put(key, value);
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        LOG_INFO("Insert benchmark completed:");
        LOG_INFO("  Operations: %d", num_ops);
        LOG_INFO("  Time: %lld ms", duration.count());
        LOG_INFO("  Ops/sec: %.2f", (num_ops * 1000.0 / duration.count()));
    }

    void bench_get(int num_ops)
    {
        LOG_INFO("Running get benchmark for %d operations...", num_ops);

        for (int i = 0; i < num_ops; i++)
        {
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i);
            lsm.put(key, value);
        }
        lsm.manual_flush();

        auto start = std::chrono::high_resolution_clock::now();

        int found = 0;
        for (int i = 0; i < num_ops; i++)
        {
            std::string key = "key_" + std::to_string(i);
            std::string value = lsm.get(key);
            if (!value.empty())
            {
                found++;
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        LOG_INFO("Get benchmark completed:");
        LOG_INFO("  Operations: %d", num_ops);
        LOG_INFO("  Found: %d", found);
        LOG_INFO("  Time: %lld ms", duration.count());
        LOG_INFO("  Ops/sec: %.2f", (num_ops * 1000.0 / duration.count()));
    }

    void bench_scan(int num_ranges, int range_size)
    {
        LOG_INFO("Running scan benchmark for %d ranges of size %d...", num_ranges, range_size);

        for (int i = 0; i < 1000; i++)
        {
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i);
            lsm.put(key, value);
        }

        auto start = std::chrono::high_resolution_clock::now();

        int total_results = 0;
        for (int i = 0; i < num_ranges; i++)
        {
            std::string start_key = "key_" + std::to_string(i * 10);
            std::string end_key = "key_" + std::to_string(i * 10 + range_size);

            auto results = lsm.scan(start_key, end_key, range_size);
            total_results += results.size();
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        LOG_INFO("Scan benchmark completed:");
        LOG_INFO("  Ranges: %d", num_ranges);
        LOG_INFO("  Total results: %d", total_results);
        LOG_INFO("  Time: %lld ms", duration.count());
        if (duration.count() > 0)
        {
            LOG_INFO("  Ranges/sec: %.2f", (num_ranges * 1000.0 / duration.count()));
        }
        else
        {
            LOG_INFO("  Ranges/sec: very fast");
        }
    }

    void bench_random_operations(int num_ops, int seed = 42, int max_key = 100, const std::string &output_file = "stats.csv")
    {
        LOG_INFO("Running random operations benchmark for %d operations (seed: %d, max_key: %d)...", num_ops, seed, max_key);

        std::mt19937 gen(seed);
        std::uniform_int_distribution<> op_dist(0, 2);
        std::uniform_int_distribution<> key_dist(0, max_key);

        std::ofstream csv_file;
        if (!output_file.empty())
        {
            csv_file.open(output_file);
            csv_file << "operation,key,time_us\n";
        }

        for (int i = 0; i < num_ops; i++)
        {
            int operation = op_dist(gen);
            std::string key = "key_" + std::to_string(key_dist(gen));

            auto op_start = std::chrono::high_resolution_clock::now();

            switch (operation)
            {
            case 0:
                lsm.put(key, "value_" + std::to_string(i));
                break;
            case 1:
                lsm.get(key);
                break;
            case 2:
                lsm.remove(key);
                break;
            }

            auto op_end = std::chrono::high_resolution_clock::now();
            auto op_duration = std::chrono::duration_cast<std::chrono::microseconds>(op_end - op_start);

            if (csv_file.is_open())
            {
                std::string op_name;
                switch (operation)
                {
                case 0:
                    op_name = "PUT";
                    break;
                case 1:
                    op_name = "GET";
                    break;
                case 2:
                    op_name = "REMOVE";
                    break;
                }
                csv_file << op_name << "," << key << "," << op_duration.count() << "\n";
            }
        }

        if (csv_file.is_open())
        {
            csv_file.close();
            LOG_INFO("CSV results saved to: %s", output_file.c_str());
        }
    }
};

int main(int argc, char *argv[])
{
    std::string mode;
    if (argc > 1)
    {
        mode = argv[1];
    }

    if (mode == "--bench-random" && argc > 2)
    {
        int num_ops = std::stoi(argv[2]);
        int seed = (argc > 3) ? std::stoi(argv[3]) : 42;
        int max_key = (argc > 4) ? std::stoi(argv[4]) : 100;
        std::string output_file = (argc > 5) ? argv[5] : "stats.csv";

        std::filesystem::remove_all("data");
        LSMTree lsm("data");
        Benchmark bench(lsm);
        bench.bench_random_operations(num_ops, seed, max_key, output_file);
    }
    else if (mode == "--bench-insert" && argc > 2)
    {
        int num_ops = std::stoi(argv[2]);
        std::filesystem::remove_all("data");
        LSMTree lsm("data");
        Benchmark bench(lsm);
        bench.bench_insert(num_ops);
    }
    else if (mode == "--bench-get" && argc > 2)
    {
        int num_ops = std::stoi(argv[2]);
        std::filesystem::remove_all("data");
        LSMTree lsm("data");
        Benchmark bench(lsm);
        bench.bench_get(num_ops);
    }
    else if (mode == "--bench-scan" && argc > 3)
    {
        int num_ranges = std::stoi(argv[2]);
        int range_size = std::stoi(argv[3]);
        std::filesystem::remove_all("data");
        LSMTree lsm("data");
        Benchmark bench(lsm);
        bench.bench_scan(num_ranges, range_size);
    }
    else
    {
        LOG_INFO("Usage:");
        LOG_INFO("  %s --bench-random <num_ops> [seed] [max_key] [output_file]", argv[0]);
        LOG_INFO("  %s --bench-insert <num_ops>", argv[0]);
        LOG_INFO("  %s --bench-get <num_ops>", argv[0]);
        LOG_INFO("  %s --bench-scan <num_ranges> <range_size>", argv[0]);
    }

    return 0;
}
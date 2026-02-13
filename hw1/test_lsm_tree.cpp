#include "lsm_tree.h"
#include "utils.h"
#include <cassert>
#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <filesystem>
#include <map>

void test_basic_operations()
{
    LOG_INFO("Testing basic operations...");

    std::filesystem::remove_all("data");
    std::filesystem::create_directory("data");

    LSMTree tree;

    tree.put("key1", "value1");
    tree.put("key2", "value2");
    tree.put("key3", "value3");

    assert(tree.get("key1") == "value1");
    assert(tree.get("key2") == "value2");
    assert(tree.get("key3") == "value3");
    assert(tree.get("nonexistent") == "");

    LOG_INFO("Basic put/get operations passed");
}

void test_scan_operations()
{
    LOG_INFO("Testing scan operations...");

    LSMTree tree;

    for (int i = 0; i < 10; i++)
    {
        tree.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }

    auto results = tree.scan("key_2", "key_5", 10);
    assert(results.size() == 4);
    assert(results[0].first == "key_2");
    assert(results[3].first == "key_5");

    results = tree.scan("key_0", "key_9", 3);
    assert(results.size() == 3);

    LOG_INFO("Scan operations passed");
}

void test_compaction()
{
    LOG_INFO("Testing compaction...");

    std::filesystem::remove_all("data");
    std::filesystem::create_directory("data");

    LSMTree tree;

    for (int i = 0; i < 1500; i++)
    {
        tree.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }

    for (int i = 0; i < 10; i++)
    {
        std::string key = "key_" + std::to_string(i * 100);
        assert(tree.get(key) == "value_" + std::to_string(i * 100));
    }

    LOG_INFO("Compaction test passed");
}

void test_bloom_filter()
{
    LOG_INFO("Testing Bloom filter...");

    LSMTree tree;

    tree.put("key1", "value1");
    tree.put("key2", "value2");

    for (int i = 0; i < 1500; i++)
    {
        tree.put("test_key_" + std::to_string(i), "test_value_" + std::to_string(i));
    }

    assert(tree.get("key1") == "value1");
    assert(tree.get("key2") == "value2");

    assert(tree.get("nonexistent_key_12345") == "");

    LOG_INFO("Bloom filter test passed");
}

void test_data_persistence()
{
    LOG_INFO("Testing data persistence within single instance...");

    std::filesystem::remove_all("data");
    std::filesystem::create_directory("data");

    LSMTree tree;

    for (int i = 0; i < 500; i++)
    {
        tree.put("persist_key_" + std::to_string(i), "persist_value_" + std::to_string(i));
    }

    for (int i = 500; i < 2000; i++)
    {
        tree.put("persist_key_" + std::to_string(i), "persist_value_" + std::to_string(i));
    }

    for (int i = 0; i < 10; i++)
    {
        std::string key = "persist_key_" + std::to_string(i * 100);
        assert(tree.get(key) == "persist_value_" + std::to_string(i * 100));
    }

    LOG_INFO("Data persistence test passed");
}

void test_deep_layers()
{
    LOG_INFO("Testing deep layer search...");

    std::filesystem::remove_all("data");
    std::filesystem::create_directory("data");

    LSMTree tree;

    for (int i = 0; i < 2000; i++)
    {
        tree.put("deep_key_" + std::to_string(i), "deep_value_" + std::to_string(i));
    }

    assert(tree.get("deep_key_0") == "deep_value_0");
    assert(tree.get("deep_key_500") == "deep_value_500");
    assert(tree.get("deep_key_1000") == "deep_value_1000");
    assert(tree.get("deep_key_1500") == "deep_value_1500");
    assert(tree.get("deep_key_1999") == "deep_value_1999");

    auto results = tree.scan("deep_key_100", "deep_key_200", 200);
    LOG_DEBUG("Scan results size: " + std::to_string(results.size()));
    if (!results.empty())
    {
        LOG_DEBUG("First key: " + results[0].first);
        LOG_DEBUG("Last key: " + results.back().first);
    }
    assert(results.size() >= 50);
    assert(results[0].first == "deep_key_100");
    assert(results.back().first <= "deep_key_200");

    LOG_INFO("Deep layer search test passed");
}

void test_edge_cases()
{
    LOG_INFO("Testing edge cases...");

    LSMTree tree;

    tree.put("", "empty_key");
    tree.put("empty_value", "");

    assert(tree.get("") == "empty_key");
    assert(tree.get("empty_value") == "");

    std::string long_key(1000, 'a');
    std::string long_value(10000, 'b');
    tree.put(long_key, long_value);

    assert(tree.get(long_key) == long_value);

    LOG_INFO("Edge cases test passed");
}

void test_random_data_with_reference()
{
    LOG_INFO("Testing with random data and reference map...");

    std::filesystem::remove_all("data");
    std::filesystem::create_directory("data");

    LSMTree tree;
    std::map<std::string, std::string> reference;

    std::srand(std::time(nullptr));
    const int NUM_OPERATIONS = 1000;

    for (int i = 0; i < NUM_OPERATIONS; i++)
    {
        int key_num = std::rand() % 200;
        std::string key = "rand_key_" + std::to_string(key_num);
        std::string value = "rand_value_" + std::to_string(i);

        tree.put(key, value);
        reference[key] = value;

        if (i % 100 == 0)
        {
            int check_key = std::rand() % 200;
            std::string check_key_str = "rand_key_" + std::to_string(check_key);

            std::string tree_value = tree.get(check_key_str);
            std::string ref_value = reference.count(check_key_str) ? reference[check_key_str] : "";

            assert(tree_value == ref_value);
        }
    }

    for (const auto &[key, expected_value] : reference)
    {
        std::string tree_value = tree.get(key);
        assert(tree_value == expected_value);
    }

    auto tree_results = tree.scan("rand_key_050", "rand_key_150", 150);

    std::map<std::string, std::string> ref_results_map;

    auto it_low = reference.lower_bound("rand_key_050");
    auto it_high = reference.upper_bound("rand_key_150");

    int count = 0;
    for (auto it = it_low; it != it_high && count < 150; ++it, ++count)
    {
        ref_results_map[it->first] = it->second;
    }

    std::map<std::string, std::string> tree_results_map;
    for (const auto &pair : tree_results)
    {
        tree_results_map[pair.first] = pair.second;
    }

    if (tree_results_map.empty() && ref_results_map.empty())
    {
    }
    else
    {
        assert(tree_results_map.size() == ref_results_map.size());

        for (const auto &[key, expected_value] : ref_results_map)
        {
            assert(tree_results_map.count(key) > 0);
            assert(tree_results_map[key] == expected_value);
        }
    }

    LOG_INFO("Random data with reference test passed");
}

void test_duplicate_keys()
{
    LOG_INFO("Testing duplicate keys (latest value should win)...");

    std::filesystem::remove_all("data");
    std::filesystem::create_directory("data");

    LSMTree tree;

    tree.put("dup_key", "value1");
    tree.put("dup_key", "value2");
    tree.put("dup_key", "value3");

    assert(tree.get("dup_key") == "value3");

    for (int i = 0; i < 1000; i++)
    {
        tree.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }

    assert(tree.get("dup_key") == "value3");

    LOG_INFO("Duplicate keys test passed");
}

void test_deletion()
{
    LOG_INFO("Testing deletion with tombstone...");

    std::filesystem::remove_all("data");
    std::filesystem::create_directory("data");

    LSMTree tree;

    tree.put("key_to_delete", "value_to_delete");
    assert(tree.get("key_to_delete") == "value_to_delete");

    tree.remove("key_to_delete");
    assert(tree.get("key_to_delete") == "");

    tree.put("disk_key", "disk_value");
    tree.manual_flush();
    assert(tree.get("disk_key") == "disk_value");

    tree.remove("disk_key");
    assert(tree.get("disk_key") == "");

    tree.put("compaction_key", "compaction_value");
    tree.manual_flush();

    for (int i = 0; i < 100; i++)
    {
        tree.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    tree.manual_flush();

    tree.remove("compaction_key");
    assert(tree.get("compaction_key") == "");

    tree.put("reinsert_key", "first_value");
    tree.remove("reinsert_key");
    tree.put("reinsert_key", "second_value");
    assert(tree.get("reinsert_key") == "second_value");

    tree.put("scan_key1", "scan_value1");
    tree.put("scan_key2", "scan_value2");
    tree.put("scan_key3", "scan_value3");
    tree.remove("scan_key2");

    auto scan_results = tree.scan("scan_key1", "scan_key3");
    assert(scan_results.size() == 2);
    assert(scan_results[0].first == "scan_key1");
    assert(scan_results[1].first == "scan_key3");

    LOG_INFO("Deletion test passed");
}

void test_comprehensive_random_operations()
{
    LOG_INFO("Testing comprehensive random operations with tier statistics...");

    std::filesystem::remove_all("data");
    std::filesystem::create_directory("data");

    LSMTree tree;
    std::map<std::string, std::string> reference_map;

    const int TOTAL_OPERATIONS = 1000;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> op_dist(0, 3);
    std::uniform_int_distribution<> key_dist(0, 200);
    std::uniform_int_distribution<> value_dist(1000, 9999);

    int puts = 0, gets = 0, removes = 0, scans = 0;

    for (int i = 0; i < TOTAL_OPERATIONS; i++)
    {
        int operation = op_dist(gen);

        switch (operation)
        {
        case 0:
        {
            std::string key = "key_" + std::to_string(key_dist(gen));
            std::string value = "value_" + std::to_string(value_dist(gen));

            tree.put(key, value);
            reference_map[key] = value;
            puts++;
            break;
        }
        case 1:
        {
            std::string key = "key_" + std::to_string(key_dist(gen));
            std::string tree_value = tree.get(key);
            std::string ref_value = reference_map.count(key) ? reference_map[key] : "";

            assert(tree_value == ref_value);
            gets++;
            break;
        }
        case 2:
        {
            std::string key = "key_" + std::to_string(key_dist(gen));
            tree.remove(key);
            reference_map.erase(key);
            removes++;
            break;
        }
        case 3:
        {
            int start_key = key_dist(gen) / 2;
            int end_key = start_key + 10;
            std::string start = "key_" + std::to_string(start_key);
            std::string end = "key_" + std::to_string(end_key);

            auto tree_results = tree.scan(start, end);

            std::vector<std::pair<std::string, std::string>> ref_results;
            for (const auto &[k, v] : reference_map)
            {
                if (k >= start && k <= end)
                {
                    ref_results.push_back({k, v});
                }
            }

            std::sort(ref_results.begin(), ref_results.end(),
                      [](const auto &a, const auto &b)
                      { return a.first < b.first; });

            assert(tree_results.size() == ref_results.size());
            for (size_t j = 0; j < tree_results.size(); j++)
            {
                assert(tree_results[j].first == ref_results[j].first);
                assert(tree_results[j].second == ref_results[j].second);
            }
            scans++;
            break;
        }
        }
    }
    {
        LOG_INFO("  Operations: PUT=%d, GET=%d, REMOVE=%d, SCAN=%d", puts, gets, removes, scans);

        tree.print_stats();

        LOG_INFO("  Verifying data integrity...");
        for (const auto &[key, expected_value] : reference_map)
        {
            std::string actual_value = tree.get(key);
            assert(actual_value == expected_value);
        }
        LOG_INFO("  Data integrity verified successfully");
    }

    LOG_INFO("Comprehensive random operations test passed");
    LOG_INFO("Final number of tiers: %d", tree.get_tier_count());
}

int main()
{
    LOG_INFO("Starting comprehensive LSM-tree tests...");

    try
    {
        test_basic_operations();
        test_scan_operations();
        test_compaction();
        test_bloom_filter();
        test_data_persistence();
        test_deep_layers();
        test_random_data_with_reference();
        test_duplicate_keys();
        test_deletion();
        test_edge_cases();
        test_comprehensive_random_operations();

        LOG_INFO("ALL TESTS PASSED");

        return 0;
    }
    catch (const std::exception &e)
    {
        LOG_ERROR("Test failed: %s", e.what());
        return 1;
    }
}
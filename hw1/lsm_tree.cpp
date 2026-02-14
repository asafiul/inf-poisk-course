#include "lsm_tree.h"
#include "utils.h"

#include <fstream>
#include <iostream>
#include <chrono>
#include <filesystem>

#ifdef TEST_SMALL_SIZE
const int TIER_COMPACTION_THRESHOLD = 2;
#else
const int TIER_COMPACTION_THRESHOLD = 10;
#endif

LSMTree::LSMTree(const std::string &dir) : data_dir(dir)
{
    memtable = std::make_unique<MemTable>();
    tiers.resize(1);
    std::filesystem::create_directories(data_dir);
}

LSMTree::~LSMTree() = default;

void LSMTree::put(const std::string &key, const std::string &value)
{
    memtable->put(key, value);

    if (memtable->should_flush())
    {
        flush_memtable();
    }
}

std::string LSMTree::get(const std::string &key)
{
    std::string value;

    if (memtable->get(key, value))
    {
        if (value == TOMBSTONE)
        {
            return "";
        }
        return value;
    }

    for (int t = 0; t < tiers.size(); t++)
    {
        for (auto it = tiers[t].rbegin(); it != tiers[t].rend(); ++it)
        {
            SSTable *sst = it->get();
            if (sst->get(key, value))
            {
                if (value == TOMBSTONE)
                {
                    return "";
                }
                return value;
            }
        }
    }

    return "";
}

void LSMTree::remove(const std::string &key)
{
    memtable->put(key, TOMBSTONE);

    if (memtable->should_flush())
    {
        flush_memtable();
    }
}

std::vector<std::pair<std::string, std::string>> LSMTree::scan(const std::string &start, const std::string &end, int limit)
{
    std::vector<std::unique_ptr<SSTableIterator>> iterators;

    for (int t = static_cast<int>(tiers.size()) - 1; t >= 0; t--)
    {
        for (auto it = tiers[t].begin(); it != tiers[t].end(); ++it)
        {
            SSTable *sst = it->get();
            auto iterator = std::make_unique<SSTableIterator>(sst->get_filename(), t);

            while (iterator->has_next())
            {
                auto [key, value] = iterator->next();
                if (key >= start && key <= end)
                {
                    iterators.push_back(std::move(iterator));
                    break;
                }
                else if (key > end)
                {
                    break;
                }
            }
        }
    }

    auto mem_results = memtable->scan(start, end, limit);

    using HeapEntry = std::tuple<std::string, std::string, size_t, size_t>;
    auto cmp = [](const HeapEntry &a, const HeapEntry &b)
    {
        if (std::get<0>(a) != std::get<0>(b))
        {
            return std::get<0>(a) > std::get<0>(b);
        }
        return std::get<3>(a) > std::get<3>(b);
    };
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, decltype(cmp)> heap(cmp);

    for (size_t i = 0; i < iterators.size(); i++)
    {
        if (iterators[i]->has_next())
        {
            auto [key, value] = iterators[i]->next();
            heap.push({key, value, i, iterators[i]->get_order()});
        }
    }

    for (size_t i = 0; i < mem_results.size(); i++)
    {
        heap.push({mem_results[i].first, mem_results[i].second, iterators.size() + i, tiers.size()});
    }

    std::vector<std::pair<std::string, std::string>> result;
    std::string last_key;

    while (!heap.empty() && result.size() < limit)
    {
        auto [key, value, iterator_idx, order] = heap.top();
        heap.pop();

        if (key != last_key)
        {
            if (value != TOMBSTONE)
            {
                result.emplace_back(key, value);
            }
            last_key = key;
        }

        if (iterator_idx < iterators.size())
        {
            if (iterators[iterator_idx]->has_next())
            {
                auto [next_key, next_value] = iterators[iterator_idx]->next();
                if (next_key <= end)
                {
                    heap.push({next_key, next_value, iterator_idx, order});
                }
            }
        }
    }

    return result;
}

void LSMTree::flush_memtable()
{
    if (memtable->size() == 0)
        return;

    LOG_DEBUG("Flushing MemTable (%zu bytes)", memtable->size());

    auto sorted_data = memtable->get_sorted_data();
    std::string filename = generate_sstable_filename();
    std::unique_ptr<SSTable> sst(SSTable::create_from_sorted_data(filename, sorted_data));

    if (sst)
    {
        tiers[0].push_back(std::move(sst));
        memtable->clear();

        compact_tier(0);
    }
}

void LSMTree::manual_flush()
{
    flush_memtable();
}

void LSMTree::print_stats() const
{
    LOG_DEBUG("LSM Tree Stats:");
    LOG_DEBUG("  MemTable size: %zu bytes", memtable->size());
    LOG_DEBUG("  Tiers: %zu", tiers.size());
    for (size_t i = 0; i < tiers.size(); i++)
    {
        LOG_DEBUG("  Tier %zu: %zu files", i, tiers[i].size());
    }
}

void LSMTree::compact_tier(int tier)
{
    if (tiers[tier].size() < TIER_COMPACTION_THRESHOLD)
    {
        return;
    }

    LOG_DEBUG("Compacting tier %zu with %zu files", tier, tiers[tier].size());

    if (tier + 1 >= tiers.size())
    {
        tiers.resize(tier + 2);
    }

    std::string new_filename = generate_sstable_filename();
    std::unique_ptr<SSTable> merged = merge_sstables(tiers[tier], new_filename);

    if (merged)
    {
        for (auto &sst : tiers[tier])
        {
            std::filesystem::remove(sst->get_filename());
        }
        tiers[tier].clear();

        tiers[tier + 1].push_back(std::move(merged));

        compact_tier(tier + 1);
    }
}

std::unique_ptr<SSTable> LSMTree::merge_sstables(const std::vector<std::unique_ptr<SSTable>> &sstables, const std::string &new_filename)
{
    LOG_DEBUG("Merging %zu SSTables using external merge sort", sstables.size());

    std::vector<std::unique_ptr<SSTableIterator>> iterators;
    for (size_t i = 0; i < sstables.size(); i++)
    {
        size_t order = sstables.size() - 1 - i;
        iterators.push_back(std::make_unique<SSTableIterator>(sstables[i]->get_filename(), order));
    }

    using HeapEntry = std::tuple<std::string, std::string, size_t, size_t>;
    auto cmp = [](const HeapEntry &a, const HeapEntry &b)
    {
        if (std::get<0>(a) != std::get<0>(b))
        {
            return std::get<0>(a) > std::get<0>(b);
        }
        return std::get<3>(a) > std::get<3>(b);
    };
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, decltype(cmp)> heap(cmp);

    for (size_t i = 0; i < iterators.size(); i++)
    {
        if (iterators[i]->has_next())
        {
            auto [key, value] = iterators[i]->next();
            heap.push({key, value, i, iterators[i]->get_order()});
        }
    }

    std::vector<std::pair<std::string, std::string>> merged_data;

    while (!heap.empty())
    {
        auto [key, value, iterator_idx, file_order] = heap.top();
        heap.pop();

        std::string current_key = key;
        std::string latest_value = value;
        size_t latest_order = file_order;

        while (!heap.empty() && std::get<0>(heap.top()) == current_key)
        {
            auto [dup_key, dup_value, dup_iterator_idx, dup_order] = heap.top();
            heap.pop();

            if (dup_order < latest_order)
            {
                latest_value = dup_value;
                latest_order = dup_order;
            }

            if (iterators[dup_iterator_idx]->has_next())
            {
                auto [next_key, next_value] = iterators[dup_iterator_idx]->next();
                heap.push({next_key, next_value, dup_iterator_idx, iterators[dup_iterator_idx]->get_order()});
            }
        }

        merged_data.push_back({current_key, latest_value});

        if (iterators[iterator_idx]->has_next())
        {
            auto [next_key, next_value] = iterators[iterator_idx]->next();
            heap.push({next_key, next_value, iterator_idx, iterators[iterator_idx]->get_order()});
        }
    }

    LOG_DEBUG("Total unique keys after merge: %zu", merged_data.size());

    std::unique_ptr<SSTable> merged(SSTable::create_from_sorted_data(new_filename, merged_data));
    LOG_DEBUG("Created merged SSTable: %s", new_filename.c_str());

    return merged;
}

int LSMTree::get_tier_count() const
{
    return tiers.size();
}

std::string LSMTree::generate_sstable_filename() const
{
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    return data_dir + "/sst_" + std::to_string(timestamp) + ".sst";
}

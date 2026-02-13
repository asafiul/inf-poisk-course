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
    std::map<std::string, std::string> result_map;

    for (int t = static_cast<int>(tiers.size()) - 1; t >= 0; t--)
    {
        for (auto it = tiers[t].begin(); it != tiers[t].end(); ++it)
        {
            SSTable *sst = it->get();
            auto sst_results = sst->scan(start, end, limit);

            for (const auto &[key, value] : sst_results)
            {
                if (value != TOMBSTONE)
                {
                    result_map[key] = value;
                }
                else
                {
                    result_map.erase(key);
                }
            }
        }
    }

    auto mem_results = memtable->scan(start, end, limit);
    for (const auto &[key, value] : mem_results)
    {
        if (value != TOMBSTONE)
        {
            result_map[key] = value;
        }
        else
        {
            result_map.erase(key);
        }
    }

    std::vector<std::pair<std::string, std::string>> result;
    for (const auto &[key, value] : result_map)
    {
        if (key >= start && key <= end)
        {
            result.emplace_back(key, value);
            if (result.size() >= limit)
                break;
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

class SSTableIterator
{
private:
    std::ifstream file;
    uint32_t num_entries;
    uint32_t current_entry;
    uint64_t data_start;
    size_t file_order;

public:
    SSTableIterator(const std::string &filename, size_t order) : current_entry(0), file_order(order)
    {
        file.open(filename, std::ios::binary);
        if (file)
        {
            uint32_t magic, bloom_offset, index_offset, bloom_size;
            magic = read_uint32(file);
            num_entries = read_uint32(file);
            bloom_offset = read_uint32(file);
            index_offset = read_uint32(file);
            bloom_size = read_uint32(file);

            if (magic == SSTABLE_MAGIC)
            {
                data_start = sizeof(uint32_t) * 5;
                file.seekg(data_start);
            }
            else
            {
                num_entries = 0;
            }
        }
        else
        {
            num_entries = 0;
        }
    }

    bool has_next() const
    {
        return current_entry < num_entries;
    }

    std::pair<std::string, std::string> next()
    {
        if (!has_next())
        {
            return {"", ""};
        }

        uint32_t key_size = read_uint32(file);
        uint32_t value_size = read_uint32(file);

        std::string key(key_size, '\0');
        file.read(&key[0], key_size);

        std::string value(value_size, '\0');
        file.read(&value[0], value_size);

        current_entry++;
        return {key, value};
    }

    size_t get_order() const { return file_order; }

    ~SSTableIterator()
    {
        if (file.is_open())
        {
            file.close();
        }
    }
};

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

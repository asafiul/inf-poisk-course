#pragma once

#include "memtable.h"
#include "sstable.h"

#include <string>
#include <vector>
#include <map>
#include <memory>

class LSMTree
{
private:
    std::unique_ptr<MemTable> memtable;
    std::vector<std::vector<std::unique_ptr<SSTable>>> tiers;
    std::string data_dir;

    void flush_memtable();
    void compact_tier(int tier);
    std::unique_ptr<SSTable> merge_sstables(const std::vector<std::unique_ptr<SSTable>> &sstables, const std::string &new_filename);
    std::string generate_sstable_filename() const;

public:
    LSMTree(const std::string &dir = "data");
    ~LSMTree();
    void put(const std::string &key, const std::string &value);
    std::string get(const std::string &key);
    void remove(const std::string &key);
    std::vector<std::pair<std::string, std::string>> scan(const std::string &start, const std::string &end, int limit = 1000);
    void print_stats() const;
    void manual_flush();
    int get_tier_count() const;
};

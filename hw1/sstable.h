#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <memory>
#include "bloom_filter.h"
#include "utils.h"

class SSTableIterator
{
private:
    std::ifstream file;
    uint32_t num_entries;
    uint32_t current_entry;
    uint64_t data_start;
    size_t file_order;

public:
    SSTableIterator(const std::string &filename, size_t order);
    ~SSTableIterator();
    bool has_next() const;
    std::pair<std::string, std::string> next();
    size_t get_order() const;
};

class SSTable
{
private:
    std::string filename;
    BloomFilter *bloom_filter;
    std::vector<std::pair<std::string, uint64_t>> sparse_index;
    size_t num_entries;

public:
    SSTable(const std::string &fname);
    ~SSTable();

    static SSTable *create_from_sorted_data(const std::string &filename,
                                            const std::vector<std::pair<std::string, std::string>> &data);

    bool get(const std::string &key, std::string &value) const;
    std::vector<std::pair<std::string, std::string>> scan(const std::string &start, const std::string &end, int limit = 1000) const;
    const std::string &get_filename() const;
};
#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include "bloom_filter.h"

class SSTable {
private:
    std::string filename;
    BloomFilter* bloom_filter;
    std::vector<std::pair<std::string, uint64_t>> sparse_index;
    size_t num_entries;
    
public:
    SSTable(const std::string& fname);
    ~SSTable();
    
    static SSTable* create_from_sorted_data(const std::string& filename, 
                                           const std::vector<std::pair<std::string, std::string>>& data);
    
    bool get(const std::string& key, std::string& value) const;
    std::vector<std::pair<std::string, std::string>> scan(const std::string& start, const std::string& end, int limit = 1000) const;
    const std::string& get_filename() const;
};
#pragma once

#include <string>
#include <vector>
#include <map>

class MemTable
{
private:
    std::map<std::string, std::string> data;
    size_t size_bytes;

public:
    MemTable();
    void put(const std::string &key, const std::string &value);
    bool get(const std::string &key, std::string &value) const;
    std::vector<std::pair<std::string, std::string>> scan(const std::string &start, const std::string &end, int limit = 1000) const;
    size_t size() const;
    bool should_flush() const;
    std::vector<std::pair<std::string, std::string>> get_sorted_data() const;
    void clear();
};

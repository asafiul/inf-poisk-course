#pragma once

#include <string>
#include <vector>
#include <cstdint>

class BloomFilter
{
private:
    std::vector<bool> bits;
    size_t size;
    int num_hashes;

    size_t hash(const std::string &key, int seed) const;

public:
    BloomFilter(size_t filter_size = 1024 * 1024, int hashes = 3);
    void add(const std::string &key);
    bool might_contain(const std::string &key) const;
    std::vector<uint8_t> serialize() const;
    void deserialize(const std::vector<uint8_t> &data);
};

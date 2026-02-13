#include "bloom_filter.h"
#include <algorithm>

const size_t BLOOM_FILTER_SIZE = 1024 * 1024;

BloomFilter::BloomFilter(size_t filter_size, int hashes)
    : size(filter_size), num_hashes(hashes)
{
    bits.resize(size, false);
}

size_t BloomFilter::hash(const std::string &key, int seed) const
{
    size_t h = 0;
    for (char c : key)
    {
        h = h * seed + c;
    }
    return h % size;
}

void BloomFilter::add(const std::string &key)
{
    for (int i = 0; i < num_hashes; i++)
    {
        size_t pos = hash(key, i + 1);
        bits[pos] = true;
    }
}

bool BloomFilter::might_contain(const std::string &key) const
{
    for (int i = 0; i < num_hashes; i++)
    {
        size_t pos = hash(key, i + 1);
        if (!bits[pos])
        {
            return false;
        }
    }
    return true;
}

std::vector<uint8_t> BloomFilter::serialize() const
{
    std::vector<uint8_t> result;
    for (size_t i = 0; i < size; i += 8)
    {
        uint8_t byte = 0;
        for (int j = 0; j < 8 && i + j < size; j++)
        {
            if (bits[i + j])
            {
                byte |= (1 << j);
            }
        }
        result.push_back(byte);
    }
    return result;
}

void BloomFilter::deserialize(const std::vector<uint8_t> &data)
{
    for (size_t i = 0; i < data.size(); i++)
    {
        for (int j = 0; j < 8 && i * 8 + j < size; j++)
        {
            bits[i * 8 + j] = (data[i] >> j) & 1;
        }
    }
}

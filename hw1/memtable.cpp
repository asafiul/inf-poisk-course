#include "memtable.h"

#ifdef TEST_SMALL_SIZE
const size_t MEMTABLE_SIZE_LIMIT = 256;
#else
const size_t MEMTABLE_SIZE_LIMIT = 4 * 1024 * 1024;
#endif

MemTable::MemTable() : size_bytes(0) {}

void MemTable::put(const std::string &key, const std::string &value)
{
    auto it = data.find(key);
    if (it != data.end())
    {
        size_bytes -= it->second.size();
    }

    data[key] = value;
    size_bytes += key.size() + value.size();
}

bool MemTable::get(const std::string &key, std::string &value) const
{
    auto it = data.find(key);
    if (it != data.end())
    {
        value = it->second;
        return true;
    }
    return false;
}

std::vector<std::pair<std::string, std::string>> MemTable::scan(const std::string &start, const std::string &end, int limit) const
{
    std::vector<std::pair<std::string, std::string>> result;
    auto it = data.lower_bound(start);

    while (it != data.end() && it->first <= end && result.size() < limit)
    {
        result.push_back(*it);
        ++it;
    }

    return result;
}

size_t MemTable::size() const
{
    return size_bytes;
}

bool MemTable::should_flush() const
{
    return size_bytes >= MEMTABLE_SIZE_LIMIT;
}

std::vector<std::pair<std::string, std::string>> MemTable::get_sorted_data() const
{
    return std::vector<std::pair<std::string, std::string>>(data.begin(), data.end());
}

void MemTable::clear()
{
    data.clear();
    size_bytes = 0;
}

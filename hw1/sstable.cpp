#include "sstable.h"
#include "utils.h"
#include <fstream>
#include <iostream>
#include <algorithm>
#include <filesystem>

SSTable::SSTable(const std::string &fname) : filename(fname), bloom_filter(nullptr), num_entries(0)
{
    bloom_filter = new BloomFilter();
}

SSTable::~SSTable()
{
    delete bloom_filter;
}

SSTable *SSTable::create_from_sorted_data(const std::string &filename,
                                          const std::vector<std::pair<std::string, std::string>> &data)
{
    SSTable *sst = new SSTable(filename);
    sst->num_entries = data.size();

    std::ofstream file(filename, std::ios::binary);
    if (!file)
    {
        std::cerr << "Cannot create SSTable file: " << filename << std::endl;
        delete sst;
        return nullptr;
    }

    uint32_t header_size = sizeof(uint32_t) * 3;
    file.seekp(header_size);

    uint64_t current_offset = header_size;
    for (size_t i = 0; i < data.size(); i++)
    {
        const auto &[key, value] = data[i];

        sst->bloom_filter->add(key);

        uint32_t key_size = key.size();
        uint32_t value_size = value.size();

        write_uint32(file, key_size);
        write_uint32(file, value_size);
        file.write(key.c_str(), key_size);
        file.write(value.c_str(), value_size);

        current_offset += sizeof(key_size) + sizeof(value_size) + key_size + value_size;
    }

    auto bloom_data = sst->bloom_filter->serialize();
    uint32_t bloom_offset = current_offset;
    uint32_t bloom_size = bloom_data.size();
    file.write(reinterpret_cast<const char *>(bloom_data.data()), bloom_size);

    file.seekp(0);
    uint32_t magic = SSTABLE_MAGIC;
    write_uint32(file, magic);
    write_uint32(file, sst->num_entries);
    write_uint32(file, bloom_offset);

    file.close();
    return sst;
}

bool SSTable::get(const std::string &key, std::string &value) const
{
    if (!bloom_filter->might_contain(key))
    {
        return false;
    }

    std::ifstream file(filename, std::ios::binary);
    if (!file)
    {
        return false;
    }

    uint32_t magic, num_entries, bloom_offset;
    magic = read_uint32(file);
    num_entries = read_uint32(file);
    bloom_offset = read_uint32(file);

    int left = 0, right = num_entries - 1;

    while (left <= right)
    {
        int mid = left + (right - left) / 2;

        uint64_t mid_offset = sizeof(uint32_t) * 3;
        file.seekg(mid_offset);

        for (int i = 0; i < mid; i++)
        {
            uint32_t key_size = read_uint32(file);
            uint32_t value_size = read_uint32(file);
            file.seekg(key_size + value_size, std::ios::cur);
        }

        uint32_t key_size = read_uint32(file);
        uint32_t value_size = read_uint32(file);
        std::string current_key(key_size, '\0');
        file.read(&current_key[0], key_size);

        if (current_key == key)
        {
            value.resize(value_size);
            file.read(&value[0], value_size);
            return true;
        }
        else if (current_key < key)
        {
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }

    return false;
}

std::vector<std::pair<std::string, std::string>> SSTable::scan(const std::string &start, const std::string &end, int limit) const
{
    std::vector<std::pair<std::string, std::string>> result;

    std::ifstream file(filename, std::ios::binary);
    if (!file)
        return result;

    uint32_t magic, num_entries, bloom_offset;
    magic = read_uint32(file);
    num_entries = read_uint32(file);
    bloom_offset = read_uint32(file);

    uint64_t start_offset = sizeof(uint32_t) * 3;

    file.seekg(start_offset);
    while (static_cast<uint64_t>(file.tellg()) < bloom_offset && result.size() < limit)
    {
        uint32_t key_size, value_size;
        key_size = read_uint32(file);
        value_size = read_uint32(file);

        std::string key(key_size, '\0');
        file.read(&key[0], key_size);

        if (key >= start)
        {
            if (key > end)
                break;

            std::string value(value_size, '\0');
            file.read(&value[0], value_size);
            result.emplace_back(key, value);
        }
        else
        {
            file.seekg(value_size, std::ios::cur);
        }
    }

    return result;
}

const std::string &SSTable::get_filename() const
{
    return filename;
}

SSTableIterator::SSTableIterator(const std::string &filename, size_t order) : current_entry(0), file_order(order)
{
    file.open(filename, std::ios::binary);
    if (file)
    {
        uint32_t magic, bloom_offset;
        magic = read_uint32(file);
        num_entries = read_uint32(file);
        bloom_offset = read_uint32(file);

        if (magic == SSTABLE_MAGIC)
        {
            data_start = sizeof(uint32_t) * 3;
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

bool SSTableIterator::has_next() const
{
    return current_entry < num_entries;
}

std::pair<std::string, std::string> SSTableIterator::next()
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

size_t SSTableIterator::get_order() const { return file_order; }

SSTableIterator::~SSTableIterator()
{
    if (file.is_open())
    {
        file.close();
    }
}

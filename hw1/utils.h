#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstdio>

#ifndef PLATFORM_APPLE
#ifndef PLATFORM_LINUX
#ifdef __APPLE__
#define PLATFORM_APPLE
#elif defined(__linux__)
#define PLATFORM_LINUX
#else
#error "Unknown platform"
#endif
#endif
#endif

#ifdef PLATFORM_APPLE
#define LSMTREE_LITTLE_ENDIAN 1
#elif defined(PLATFORM_LINUX)
#define LSMTREE_LITTLE_ENDIAN 1
#else
#error "Unknown platform"
#endif

inline const std::string TOMBSTONE = "__TOMBSTONE__";
constexpr uint32_t SSTABLE_MAGIC = 0x53535442; // "SSTB"

#ifdef DEBUG
#define LOG_INFO(...)        \
    do                       \
    {                        \
        printf("[INFO] ");   \
        printf(__VA_ARGS__); \
        printf("\n");        \
    } while (0)
#define LOG_ERROR(...)                \
    do                                \
    {                                 \
        fprintf(stderr, "[ERROR] ");  \
        fprintf(stderr, __VA_ARGS__); \
        fprintf(stderr, "\n");        \
    } while (0)
#define LOG_DEBUG(...)       \
    do                       \
    {                        \
        printf("[DEBUG] ");  \
        printf(__VA_ARGS__); \
        printf("\n");        \
    } while (0)
#else
#define LOG_INFO(...)        \
    do                       \
    {                        \
        printf("[INFO] ");   \
        printf(__VA_ARGS__); \
        printf("\n");        \
    } while (0)
#define LOG_ERROR(...)                \
    do                                \
    {                                 \
        fprintf(stderr, "[ERROR] ");  \
        fprintf(stderr, __VA_ARGS__); \
        fprintf(stderr, "\n");        \
    } while (0)
#define LOG_DEBUG(...)
#endif

inline void write_uint32(std::ofstream &file, uint32_t value)
{
#if LSMTREE_LITTLE_ENDIAN
    file.write(reinterpret_cast<const char *>(&value), sizeof(value));
#else
    uint8_t bytes[4];
    bytes[0] = static_cast<uint8_t>(value & 0xFF);
    bytes[1] = static_cast<uint8_t>((value >> 8) & 0xFF);
    bytes[2] = static_cast<uint8_t>((value >> 16) & 0xFF);
    bytes[3] = static_cast<uint8_t>((value >> 24) & 0xFF);
    file.write(reinterpret_cast<const char *>(bytes), sizeof(bytes));
#endif
}

inline void write_uint64(std::ofstream &file, uint64_t value)
{
#if LSMTREE_LITTLE_ENDIAN
    file.write(reinterpret_cast<const char *>(&value), sizeof(value));
#else
    uint8_t bytes[8];
    for (int i = 0; i < 8; i++)
    {
        bytes[i] = static_cast<uint8_t>((value >> (i * 8)) & 0xFF);
    }
    file.write(reinterpret_cast<const char *>(bytes), sizeof(bytes));
#endif
}

inline uint32_t read_uint32(std::ifstream &file)
{
#if LSMTREE_LITTLE_ENDIAN
    uint32_t value;
    file.read(reinterpret_cast<char *>(&value), sizeof(value));
    return value;
#else
    uint8_t bytes[4];
    file.read(reinterpret_cast<char *>(bytes), sizeof(bytes));
    return (static_cast<uint32_t>(bytes[0]) |
            static_cast<uint32_t>(bytes[1]) << 8 |
            static_cast<uint32_t>(bytes[2]) << 16 |
            static_cast<uint32_t>(bytes[3]) << 24);
#endif
}

inline uint64_t read_uint64(std::ifstream &file)
{
#if LSMTREE_LITTLE_ENDIAN
    uint64_t value;
    file.read(reinterpret_cast<char *>(&value), sizeof(value));
    return value;
#else
    uint8_t bytes[8];
    file.read(reinterpret_cast<char *>(bytes), sizeof(bytes));
    uint64_t value = 0;
    for (int i = 0; i < 8; i++)
    {
        value |= static_cast<uint64_t>(bytes[i]) << (i * 8);
    }
    return value;
#endif
}

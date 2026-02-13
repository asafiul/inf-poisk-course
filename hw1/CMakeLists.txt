cmake_minimum_required(VERSION 3.10)
project(LSMTree)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Определяем платформу
if(APPLE)
    add_compile_definitions(PLATFORM_APPLE)
    message(STATUS "Building for macOS")
elseif(UNIX)
    add_compile_definitions(PLATFORM_LINUX)
    message(STATUS "Building for Linux")
else()
    message(FATAL_ERROR "Unsupported platform")
endif()

# Опции сборки
option(DEBUG "Build with debug output" OFF)
option(ENABLE_TESTS "Build tests" ON)
option(TEST_SMALL_SIZE "Use small sizes for fast testing" ON)

# Флаги компиляции
if(DEBUG)
    add_compile_definitions(DEBUG)
    set(CMAKE_BUILD_TYPE Debug)
    message(STATUS "Debug build enabled - verbose logging")
else()
    set(CMAKE_BUILD_TYPE Release)
    message(STATUS "Release build - minimal logging")
endif()

if(TEST_SMALL_SIZE)
    add_compile_definitions(TEST_SMALL_SIZE)
    message(STATUS "Using small test sizes (2KB MemTable, 2-file compaction)")
else()
    message(STATUS "Using default sizes (4MB MemTable, 10-file compaction)")
endif()

# Основной исполняемый файл
add_executable(lsm_tree
    main.cpp
    lsm_tree.cpp
    bloom_filter.cpp
    memtable.cpp
    sstable.cpp
)

# Тестовый исполняемый файл
add_executable(test_lsm_tree
    test_lsm_tree.cpp
    lsm_tree.cpp
    bloom_filter.cpp
    memtable.cpp
    sstable.cpp
)

# Тесты если включены
if(ENABLE_TESTS)
    # TODO: Добавить Google Test позже
endif()
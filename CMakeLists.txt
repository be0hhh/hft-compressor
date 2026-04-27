cmake_minimum_required(VERSION 3.20)
project(hft-compressor LANGUAGES CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(HFT_COMPRESSOR_BUILD_CLI ON CACHE BOOL "Build hft-compressor CLI")
set(HFT_COMPRESSOR_BUILD_TESTS ON CACHE BOOL "Build hft-compressor tests")

find_package(Threads REQUIRED)
find_package(PkgConfig QUIET)

set(HFT_COMPRESSOR_WITH_ZSTD OFF)
if(PkgConfig_FOUND)
    pkg_check_modules(ZSTD QUIET IMPORTED_TARGET libzstd)
endif()
if(TARGET PkgConfig::ZSTD)
    set(HFT_COMPRESSOR_WITH_ZSTD ON)
else()
    find_path(ZSTD_INCLUDE_DIR zstd.h)
    find_library(ZSTD_LIBRARY zstd)
    if(ZSTD_INCLUDE_DIR AND ZSTD_LIBRARY)
        set(HFT_COMPRESSOR_WITH_ZSTD ON)
    endif()
endif()

add_library(hft_compressor_core SHARED
    src/codecs/zstd_jsonl_blocks/ZstdJsonlBlocks.cpp
    src/common/CompressionInternals.cpp
    src/common/timing.cpp
    src/compressor.cpp
    src/container/hfc/format.cpp
    src/decoder.cpp
    src/metrics.cpp
    src/metrics_server.cpp
    src/pipelines/PipelineRegistry.cpp
    src/stream_type.cpp
)
target_include_directories(hft_compressor_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_features(hft_compressor_core PUBLIC cxx_std_23)
target_compile_definitions(hft_compressor_core PUBLIC HFT_COMPRESSOR_WITH_ZSTD=$<BOOL:${HFT_COMPRESSOR_WITH_ZSTD}> HFT_COMPRESSOR_SHARED PRIVATE HFT_COMPRESSOR_BUILDING_LIBRARY)
target_link_libraries(hft_compressor_core PUBLIC Threads::Threads)
if(TARGET PkgConfig::ZSTD)
    target_link_libraries(hft_compressor_core PUBLIC PkgConfig::ZSTD)
elseif(HFT_COMPRESSOR_WITH_ZSTD)
    target_include_directories(hft_compressor_core PUBLIC ${ZSTD_INCLUDE_DIR})
    target_link_libraries(hft_compressor_core PUBLIC ${ZSTD_LIBRARY})
endif()

if(HFT_COMPRESSOR_BUILD_CLI)
    add_executable(hft-compressor src/main.cpp)
    target_link_libraries(hft-compressor PRIVATE hft_compressor_core)
endif()

if(HFT_COMPRESSOR_BUILD_TESTS)
    enable_testing()
    add_executable(hft-compressor-tests tests/compressor_tests.cpp)
    target_link_libraries(hft-compressor-tests PRIVATE hft_compressor_core)
    add_test(NAME hft-compressor-tests COMMAND hft-compressor-tests)
endif()

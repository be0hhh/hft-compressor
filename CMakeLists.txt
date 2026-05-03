cmake_minimum_required(VERSION 3.20)
project(hft-compressor LANGUAGES CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(HFT_COMPRESSOR_BUILD_CLI ON CACHE BOOL "Build hft-compressor CLI")
set(HFT_COMPRESSOR_BUILD_TESTS ON CACHE BOOL "Build hft-compressor tests")

find_package(Threads REQUIRED)
find_package(PkgConfig QUIET)
find_package(ZLIB QUIET)
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

set(HFT_COMPRESSOR_WITH_LZ4 OFF)
if(PkgConfig_FOUND)
    pkg_check_modules(LZ4 QUIET IMPORTED_TARGET liblz4)
endif()
if(TARGET PkgConfig::LZ4)
    set(HFT_COMPRESSOR_WITH_LZ4 ON)
else()
    find_path(LZ4_INCLUDE_DIR lz4.h)
    find_library(LZ4_LIBRARY lz4)
    if(LZ4_INCLUDE_DIR AND LZ4_LIBRARY)
        set(HFT_COMPRESSOR_WITH_LZ4 ON)
    endif()
endif()

set(HFT_COMPRESSOR_WITH_BROTLI OFF)
if(PkgConfig_FOUND)
    pkg_check_modules(BROTLIENC QUIET IMPORTED_TARGET libbrotlienc)
    pkg_check_modules(BROTLIDEC QUIET IMPORTED_TARGET libbrotlidec)
endif()
if(TARGET PkgConfig::BROTLIENC AND TARGET PkgConfig::BROTLIDEC)
    set(HFT_COMPRESSOR_WITH_BROTLI ON)
else()
    find_path(BROTLI_INCLUDE_DIR brotli/encode.h)
    find_library(BROTLIENC_LIBRARY brotlienc)
    find_library(BROTLIDEC_LIBRARY brotlidec)
    find_library(BROTLICOMMON_LIBRARY brotlicommon)
    if(BROTLI_INCLUDE_DIR AND BROTLIENC_LIBRARY AND BROTLIDEC_LIBRARY)
        set(HFT_COMPRESSOR_WITH_BROTLI ON)
    endif()
endif()

set(HFT_COMPRESSOR_WITH_LZMA OFF)
if(PkgConfig_FOUND)
    pkg_check_modules(LZMA QUIET IMPORTED_TARGET liblzma)
endif()
if(TARGET PkgConfig::LZMA)
    set(HFT_COMPRESSOR_WITH_LZMA ON)
else()
    find_path(LZMA_INCLUDE_DIR lzma.h)
    find_library(LZMA_LIBRARY lzma)
    if(LZMA_INCLUDE_DIR AND LZMA_LIBRARY)
        set(HFT_COMPRESSOR_WITH_LZMA ON)
    endif()
endif()

set(HFT_COMPRESSOR_WITH_ZLIB OFF)
if(ZLIB_FOUND)
    set(HFT_COMPRESSOR_WITH_ZLIB ON)
endif()

add_library(hft_compressor_core SHARED
    src/c_api.cpp
    src/codecs/bookticker_delta_mask/BookTickerDeltaMask.cpp
    src/codecs/brotli_jsonl_blocks/BrotliJsonlBlocks.cpp
    src/codecs/depth_ladder_offset/DepthLadderOffset.cpp
    src/codecs/depth_ladder_offset/DepthLadderOffsetV2.cpp
    src/codecs/gzip_jsonl_blocks/GzipJsonlBlocks.cpp
    src/codecs/lz4_jsonl_blocks/Lz4JsonlBlocks.cpp
    src/codecs/raw_jsonl_blocks/RawJsonlBlocks.cpp
    src/codecs/standard_jsonl_blocks/StandardJsonlBlocks.cpp
    src/codecs/trades_grouped_delta_qtydict/TradesGroupedDeltaQtyDict.cpp
    src/codecs/xz_jsonl_blocks/XzJsonlBlocks.cpp
    src/codecs/zstd_jsonl_blocks/ZstdJsonlBlocks.cpp
    src/common/CompressionInternals.cpp
    src/common/timing.cpp
    src/compressor.cpp
    src/container/hfc/format.cpp
    src/decoder.cpp
    src/metrics.cpp
    src/metrics_server.cpp
    src/pipelines/PipelineBackend.cpp
    src/pipelines/PipelineRegistry.cpp
    src/replay_decode.cpp
    src/stream_type.cpp
)
target_include_directories(hft_compressor_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_features(hft_compressor_core PUBLIC cxx_std_23)
target_compile_definitions(hft_compressor_core
    PUBLIC
        HFT_COMPRESSOR_WITH_ZSTD=$<BOOL:${HFT_COMPRESSOR_WITH_ZSTD}>
        HFT_COMPRESSOR_WITH_LZ4=$<BOOL:${HFT_COMPRESSOR_WITH_LZ4}>
        HFT_COMPRESSOR_WITH_BROTLI=$<BOOL:${HFT_COMPRESSOR_WITH_BROTLI}>
        HFT_COMPRESSOR_WITH_LZMA=$<BOOL:${HFT_COMPRESSOR_WITH_LZMA}>
        HFT_COMPRESSOR_WITH_ZLIB=$<BOOL:${HFT_COMPRESSOR_WITH_ZLIB}>
        HFT_COMPRESSOR_SHARED
    PRIVATE
        HFT_COMPRESSOR_BUILDING_LIBRARY
)
target_link_libraries(hft_compressor_core PUBLIC Threads::Threads)
if(TARGET PkgConfig::ZSTD)
    target_link_libraries(hft_compressor_core PUBLIC PkgConfig::ZSTD)
elseif(HFT_COMPRESSOR_WITH_ZSTD)
    target_include_directories(hft_compressor_core PUBLIC ${ZSTD_INCLUDE_DIR})
    target_link_libraries(hft_compressor_core PUBLIC ${ZSTD_LIBRARY})
endif()

if(TARGET PkgConfig::LZ4)
    target_link_libraries(hft_compressor_core PUBLIC PkgConfig::LZ4)
elseif(HFT_COMPRESSOR_WITH_LZ4)
    target_include_directories(hft_compressor_core PUBLIC ${LZ4_INCLUDE_DIR})
    target_link_libraries(hft_compressor_core PUBLIC ${LZ4_LIBRARY})
endif()

if(TARGET PkgConfig::BROTLIENC AND TARGET PkgConfig::BROTLIDEC)
    target_link_libraries(hft_compressor_core PUBLIC PkgConfig::BROTLIENC PkgConfig::BROTLIDEC)
elseif(HFT_COMPRESSOR_WITH_BROTLI)
    target_include_directories(hft_compressor_core PUBLIC ${BROTLI_INCLUDE_DIR})
    target_link_libraries(hft_compressor_core PUBLIC ${BROTLIENC_LIBRARY} ${BROTLIDEC_LIBRARY})
    if(BROTLICOMMON_LIBRARY)
        target_link_libraries(hft_compressor_core PUBLIC ${BROTLICOMMON_LIBRARY})
    endif()
endif()

if(TARGET PkgConfig::LZMA)
    target_link_libraries(hft_compressor_core PUBLIC PkgConfig::LZMA)
elseif(HFT_COMPRESSOR_WITH_LZMA)
    target_include_directories(hft_compressor_core PUBLIC ${LZMA_INCLUDE_DIR})
    target_link_libraries(hft_compressor_core PUBLIC ${LZMA_LIBRARY})
endif()

if(HFT_COMPRESSOR_WITH_ZLIB)
    target_link_libraries(hft_compressor_core PUBLIC ZLIB::ZLIB)
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


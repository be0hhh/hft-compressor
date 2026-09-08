cmake_minimum_required(VERSION 3.20)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(_hft_compressor_toolchain "${CMAKE_CURRENT_LIST_DIR}/../../cmake/CxetToolchain.cmake")
  if(NOT EXISTS "${_hft_compressor_toolchain}")
    message(FATAL_ERROR "Build hft-compressor under the canonical CXET root to select its pinned compiler.")
  endif()
  include("${_hft_compressor_toolchain}")
  unset(_hft_compressor_toolchain)
endif()
project(hft-compressor LANGUAGES CXX VERSION 0.1.0)

if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(FATAL_ERROR "hft-compressor requires the pinned compiler selected by the canonical CXET root.")
endif()

set(HFT_COMPRESSOR_STANDALONE OFF)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(HFT_COMPRESSOR_STANDALONE ON)
endif()

option(HFT_COMPRESSOR_PORTABLE_BUILD "If ON: disable -march=native for hft-compressor Release builds." OFF)
if(HFT_COMPRESSOR_STANDALONE)
    set(_HFT_COMPRESSOR_BUILD_TOOLS_DEFAULT ON)
else()
    set(_HFT_COMPRESSOR_BUILD_TOOLS_DEFAULT OFF)
endif()

option(HFT_COMPRESSOR_BUILD_CLI
    "Build hft-compressor CLI"
    ${_HFT_COMPRESSOR_BUILD_TOOLS_DEFAULT})
option(HFT_COMPRESSOR_BUILD_TESTS
    "Build hft-compressor tests"
    ${_HFT_COMPRESSOR_BUILD_TOOLS_DEFAULT})
unset(_HFT_COMPRESSOR_BUILD_TOOLS_DEFAULT)

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
    src/Api/src/CApi.cpp
    src/Runtime/src/Codecs/BooktickerDeltaMask/BookTickerDeltaMask.cpp
    src/Runtime/src/Codecs/BrotliJsonlBlocks/BrotliJsonlBlocks.cpp
    src/Runtime/src/Codecs/DepthLadderOffset/DepthLadderOffset.cpp
    src/Runtime/src/Codecs/DepthLadderOffset/LegacyDepthLadderOffset.cpp
    src/Runtime/src/Codecs/EntropyHftmac/EntropyHftMac.cpp
    src/Runtime/src/Codecs/GzipJsonlBlocks/GzipJsonlBlocks.cpp
    src/Runtime/src/Codecs/Lz4JsonlBlocks/Lz4JsonlBlocks.cpp
    src/Runtime/src/Codecs/RawJsonlBlocks/RawJsonlBlocks.cpp
    src/Runtime/src/Codecs/StandardJsonlBlocks/StandardJsonlBlocks.cpp
    src/Runtime/src/Codecs/TradesGroupedDeltaQtydict/TradesGroupedDeltaQtyDict.cpp
    src/Runtime/src/Codecs/XzJsonlBlocks/XzJsonlBlocks.cpp
    src/Runtime/src/Codecs/ZstdJsonlBlocks/ZstdJsonlBlocks.cpp
    src/Runtime/src/Common/CompressionInternals.cpp
    src/Runtime/src/Common/Timing.cpp
    src/Api/src/Compressor.cpp
    src/Runtime/src/Container/Hfc/Format.cpp
    src/Api/src/Decoder.cpp
    src/Runtime/src/Metrics/Metrics.cpp
    src/Runtime/src/Metrics/CompressionResultMetrics.cpp
    src/Runtime/src/Metrics/MetricsServer.cpp
    src/Runtime/src/Pipelines/PipelineBackend.cpp
    src/Runtime/src/Pipelines/PipelineRegistry.cpp
    src/Api/src/ReplayDecode.cpp
    src/Api/src/StreamType.cpp
)
add_library(hft_compressor::core ALIAS hft_compressor_core)
set_target_properties(hft_compressor_core PROPERTIES
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(hft_compressor_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src/Api/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src/Runtime/src
)
target_compile_features(hft_compressor_core PUBLIC cxx_std_23)
if(NOT HFT_COMPRESSOR_PORTABLE_BUILD)
    target_compile_options(hft_compressor_core PRIVATE
        $<$<CONFIG:Release>:-O3>
        $<$<CONFIG:Release>:-march=native>
    )
endif()
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
    add_executable(hft-compressor src/Process/Main.cpp)
    set_target_properties(hft-compressor PROPERTIES
        CXX_STANDARD 23
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
    )
    target_link_libraries(hft-compressor PRIVATE hft_compressor_core)
    if(NOT HFT_COMPRESSOR_PORTABLE_BUILD)
        target_compile_options(hft-compressor PRIVATE
            $<$<CONFIG:Release>:-O3>
            $<$<CONFIG:Release>:-march=native>
        )
    endif()
endif()

if(HFT_COMPRESSOR_BUILD_TESTS)
    enable_testing()
    add_executable(hft-compressor-tests tests/CompressorTests.cpp)
    set_target_properties(hft-compressor-tests PROPERTIES
        CXX_STANDARD 23
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
    )
    target_link_libraries(hft-compressor-tests PRIVATE hft_compressor_core)
    if(NOT HFT_COMPRESSOR_PORTABLE_BUILD)
        target_compile_options(hft-compressor-tests PRIVATE
            $<$<CONFIG:Release>:-O3>
            $<$<CONFIG:Release>:-march=native>
        )
    endif()
    add_test(NAME hft-compressor-tests COMMAND hft-compressor-tests)
endif()

#pragma once

#if defined(_WIN32) && defined(HFT_COMPRESSOR_SHARED)
#if defined(HFT_COMPRESSOR_BUILDING_LIBRARY)
#define HFT_COMPRESSOR_API __declspec(dllexport)
#else
#define HFT_COMPRESSOR_API __declspec(dllimport)
#endif
#elif defined(__GNUC__) && defined(HFT_COMPRESSOR_SHARED)
#define HFT_COMPRESSOR_API __attribute__((visibility("default")))
#else
#define HFT_COMPRESSOR_API
#endif
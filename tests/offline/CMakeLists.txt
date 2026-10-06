cxet_add_offline_suite(hftc_offline_cases OWNER hft-compressor EXPECTED 18
  SOURCES CompressorCases.cpp ByteBlockCases.cpp LIBRARIES hft_compressor::core LABELS compression)
cxet_add_offline_suite(hftc_offline_entropy OWNER hft-compressor EXPECTED 8
  SOURCES EntropyCases.cpp LIBRARIES hft_compressor::core LABELS entropy)
target_link_options(hftc_offline_entropy PRIVATE "LINKER:--wrap=_Znwm")

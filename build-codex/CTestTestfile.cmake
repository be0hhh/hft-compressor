# CMake generated Testfile for 
# Source directory: C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor
# Build directory: C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/build-codex
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
if(CTEST_CONFIGURATION_TYPE MATCHES "^([Dd][Ee][Bb][Uu][Gg])$")
  add_test([=[hft-compressor-tests]=] "C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/build-codex/Debug/hft-compressor-tests.exe")
  set_tests_properties([=[hft-compressor-tests]=] PROPERTIES  _BACKTRACE_TRIPLES "C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/CMakeLists.txt;57;add_test;C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/CMakeLists.txt;0;")
elseif(CTEST_CONFIGURATION_TYPE MATCHES "^([Rr][Ee][Ll][Ee][Aa][Ss][Ee])$")
  add_test([=[hft-compressor-tests]=] "C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/build-codex/Release/hft-compressor-tests.exe")
  set_tests_properties([=[hft-compressor-tests]=] PROPERTIES  _BACKTRACE_TRIPLES "C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/CMakeLists.txt;57;add_test;C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/CMakeLists.txt;0;")
elseif(CTEST_CONFIGURATION_TYPE MATCHES "^([Mm][Ii][Nn][Ss][Ii][Zz][Ee][Rr][Ee][Ll])$")
  add_test([=[hft-compressor-tests]=] "C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/build-codex/MinSizeRel/hft-compressor-tests.exe")
  set_tests_properties([=[hft-compressor-tests]=] PROPERTIES  _BACKTRACE_TRIPLES "C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/CMakeLists.txt;57;add_test;C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/CMakeLists.txt;0;")
elseif(CTEST_CONFIGURATION_TYPE MATCHES "^([Rr][Ee][Ll][Ww][Ii][Tt][Hh][Dd][Ee][Bb][Ii][Nn][Ff][Oo])$")
  add_test([=[hft-compressor-tests]=] "C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/build-codex/RelWithDebInfo/hft-compressor-tests.exe")
  set_tests_properties([=[hft-compressor-tests]=] PROPERTIES  _BACKTRACE_TRIPLES "C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/CMakeLists.txt;57;add_test;C:/Users/be0h/PycharmProjects/course project/CXETCPP/apps/hft-compressor/CMakeLists.txt;0;")
else()
  add_test([=[hft-compressor-tests]=] NOT_AVAILABLE)
endif()

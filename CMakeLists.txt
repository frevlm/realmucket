cmake_minimum_required(VERSION 3.28)
project(realmucket LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(RMK_BUILD_TESTS "Build unit tests" ON)

# Native-first: every module builds when its dependencies are present on this machine.
find_package(Threads REQUIRED)
find_package(OpenSSL)
find_package(Protobuf CONFIG)
if(NOT Protobuf_FOUND)
    find_package(Protobuf)
endif()
find_package(gRPC CONFIG)
find_library(ROCKSDB_LIB rocksdb)
find_path(ROCKSDB_INC rocksdb/db.h)

set(RMK_WARNINGS -Wall -Wextra -Wpedantic -Werror)

# rmk_module(<target> <module dir> [deps...]): a static library from <dir>/src/*.cpp.
# Public headers live in <dir>/include, so a module owns its headers, sources and tests.
function(rmk_module target dir)
    file(GLOB sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/src/*.cpp)
    if(NOT sources)
        return()
    endif()
    add_library(${target} STATIC ${sources})
    target_include_directories(${target} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/include)
    target_compile_options(${target} PRIVATE ${RMK_WARNINGS})
    target_link_libraries(${target} PUBLIC ${ARGN})
endfunction()

# rmk_program(<target> <main.cpp> [deps...]): an executable from one source file.
function(rmk_program target main)
    add_executable(${target} ${CMAKE_CURRENT_SOURCE_DIR}/${main})
    target_compile_options(${target} PRIVATE ${RMK_WARNINGS})
    target_link_libraries(${target} PRIVATE ${ARGN})
    install(TARGETS ${target} RUNTIME DESTINATION bin)
endfunction()

# ----- Libraries (libs/<module>/{include,src,test})

rmk_module(rmk_core libs/core Threads::Threads)
rmk_module(rmk_stores libs/stores rmk_core)
rmk_module(rmk_local libs/local rmk_core)

if(OpenSSL_FOUND)
    rmk_module(rmk_crypto libs/crypto rmk_core OpenSSL::Crypto)
endif()

rmk_module(rmk_http libs/http rmk_core)

# Metadata store on RocksDB (the engine the benchmarks picked, docs/PERFORMANCE.md).
if(ROCKSDB_LIB AND ROCKSDB_INC)
    rmk_module(rmk_rocksdb libs/rocksdb rmk_core ${ROCKSDB_LIB})
    if(TARGET rmk_rocksdb)
        target_include_directories(rmk_rocksdb PUBLIC ${ROCKSDB_INC})
        target_include_directories(rmk_rocksdb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libs/rocksdb/src)
    endif()
endif()

set(RMK_HAVE_RPC OFF)
if(gRPC_FOUND AND Protobuf_FOUND AND TARGET rmk_crypto AND TARGET rmk_http)
    set(RMK_HAVE_RPC ON)
endif()

if(RMK_HAVE_RPC)
    set(proto_dir ${CMAKE_CURRENT_SOURCE_DIR}/proto)
    set(gen_dir ${CMAKE_CURRENT_BINARY_DIR}/gen)
    file(MAKE_DIRECTORY ${gen_dir})
    get_target_property(grpc_plugin gRPC::grpc_cpp_plugin LOCATION)
    set(proto_sources)
    foreach(name storage)
        set(proto ${proto_dir}/rmk/v1/${name}.proto)
        set(out ${gen_dir}/rmk/v1/${name})
        add_custom_command(
            OUTPUT ${out}.pb.cc ${out}.pb.h ${out}.grpc.pb.cc ${out}.grpc.pb.h
            COMMAND protobuf::protoc
            ARGS --proto_path=${proto_dir} --cpp_out=${gen_dir} --grpc_out=${gen_dir}
                 --plugin=protoc-gen-grpc=${grpc_plugin} ${proto}
            DEPENDS ${proto} protobuf::protoc)
        list(APPEND proto_sources ${out}.pb.cc ${out}.grpc.pb.cc)
    endforeach()
    add_library(rmk_proto STATIC ${proto_sources})
    target_include_directories(rmk_proto PUBLIC ${gen_dir})
    target_link_libraries(rmk_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

    # Status mapping and transport security.
    rmk_module(rmk_rpc libs/rpc rmk_proto rmk_core rmk_stores)

    # Client SDK: one public header, libs/client/include/rmk/client.hpp.
    file(GLOB client_sources CONFIGURE_DEPENDS libs/client/src/*.cpp)
    if(client_sources)
        add_library(rmk_client STATIC ${client_sources})
        target_include_directories(rmk_client PUBLIC libs/client/include)
        target_compile_options(rmk_client PRIVATE ${RMK_WARNINGS})
        target_link_libraries(rmk_client PRIVATE rmk_proto rmk_core)
        install(TARGETS rmk_client ARCHIVE DESTINATION lib)
        install(DIRECTORY libs/client/include/ DESTINATION include)
    endif()

    # ----- Services (services/<name>/{include,src,test,main.cpp})

    rmk_module(rmk_gateway_lib services/gateway rmk_rpc rmk_crypto rmk_http)
    if(TARGET rmk_rocksdb)
        rmk_module(rmk_realmucketd_lib services/realmucketd rmk_gateway_lib rmk_local rmk_rocksdb rmk_crypto rmk_http)
        rmk_program(realmucketd services/realmucketd/main.cpp rmk_realmucketd_lib)
    endif()

    # ----- Apps (apps/<name>/main.cpp)

    rmk_program(rmkctl apps/rmkctl/main.cpp rmk_client)

    # Load generator for a running Gateway or realmucketd; not installed.
    add_executable(realmucket-bench ${CMAKE_CURRENT_SOURCE_DIR}/apps/bench/main.cpp)
    target_compile_options(realmucket-bench PRIVATE ${RMK_WARNINGS})
    target_link_libraries(realmucket-bench PRIVATE rmk_client Threads::Threads)
endif()

# ----- Metadata engine micro-benchmark (tools/metabench): off by default, an engine that is not
# installed is simply left out.
option(RMK_BUILD_METABENCH "Build tools/metabench" OFF)
if(RMK_BUILD_METABENCH)
    find_package(SQLite3 REQUIRED)
    find_library(LMDB_LIBRARY lmdb)
    find_path(LMDB_INCLUDE_DIR lmdb.h)
    find_library(ROCKSDB_LIBRARY rocksdb)
    find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h)
    file(GLOB metabench_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tools/metabench/*.cpp)
    add_executable(metabench ${metabench_sources})
    target_compile_options(metabench PRIVATE -Wall -Wextra -Werror)
    if(TARGET SQLite3::SQLite3)
        target_link_libraries(metabench PRIVATE SQLite3::SQLite3 Threads::Threads)
    else()
        target_link_libraries(metabench PRIVATE SQLite::SQLite3 Threads::Threads)
    endif()
    if(LMDB_LIBRARY AND LMDB_INCLUDE_DIR)
        target_compile_definitions(metabench PRIVATE MB_HAVE_LMDB=1)
        target_include_directories(metabench PRIVATE ${LMDB_INCLUDE_DIR})
        target_link_libraries(metabench PRIVATE ${LMDB_LIBRARY})
    endif()
    if(ROCKSDB_LIBRARY AND ROCKSDB_INCLUDE_DIR)
        target_compile_definitions(metabench PRIVATE MB_HAVE_ROCKSDB=1)
        target_include_directories(metabench PRIVATE ${ROCKSDB_INCLUDE_DIR})
        target_link_libraries(metabench PRIVATE ${ROCKSDB_LIBRARY})
    endif()
endif()

# ----- Tests: one executable per module, from <dir>/*.cpp, linked to the named libraries.
# Cross-module tests (e2e) and the shared contract suites live in the top-level tests/.

if(RMK_BUILD_TESTS)
    find_package(GTest CONFIG)
    if(NOT GTest_FOUND)
        include(FetchContent)
        FetchContent_Declare(googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG 52eb8108c5bdec04579160ae17225d66034bd723 # v1.17.0
        )
        set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(googletest)
    endif()
    enable_testing()
    include(GoogleTest)

    # rmk_tests(<name> <test dir> [deps...])
    function(rmk_tests name dir)
        foreach(dep ${ARGN})
            if(NOT TARGET ${dep})
                return()
            endif()
        endforeach()
        file(GLOB sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/*.cpp)
        if(NOT sources)
            return()
        endif()
        add_executable(rmk_${name}_tests ${sources})
        target_include_directories(rmk_${name}_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_compile_options(rmk_${name}_tests PRIVATE -Wall -Wextra -Werror)
        target_link_libraries(rmk_${name}_tests PRIVATE ${ARGN} GTest::gtest_main)
        gtest_discover_tests(rmk_${name}_tests DISCOVERY_TIMEOUT 30)
    endfunction()

    rmk_tests(core libs/core/test rmk_core)
    rmk_tests(stores libs/stores/test rmk_stores)
    rmk_tests(local libs/local/test rmk_local)
    rmk_tests(crypto libs/crypto/test rmk_crypto)
    rmk_tests(http libs/http/test rmk_http)
    rmk_tests(rocksdb libs/rocksdb/test rmk_rocksdb)
    rmk_tests(rpc libs/rpc/test rmk_rpc)
    rmk_tests(gateway services/gateway/test rmk_gateway_lib rmk_local rmk_stores)
    rmk_tests(realmucketd services/realmucketd/test rmk_realmucketd_lib rmk_client rmk_stores)
    rmk_tests(e2e tests/e2e rmk_client rmk_realmucketd_lib)
endif()

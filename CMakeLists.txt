cmake_minimum_required(VERSION 3.28)
project(realmucket LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(RMK_BUILD_TESTS "Build unit tests" ON)

# Native-first: every module builds when its dependencies are present on this machine.
# Only the FoundationDB store needs libfdb_c; without it everything else still builds and
# runs against the in-memory stores.
find_package(Threads REQUIRED)
find_package(OpenSSL)
find_package(CURL)
find_package(Protobuf CONFIG)
if(NOT Protobuf_FOUND)
    find_package(Protobuf)
endif()
find_package(gRPC CONFIG)
find_path(FDB_INCLUDE_DIR foundationdb/fdb_c.h)
find_library(FDB_LIBRARY fdb_c)

set(RMK_WARNINGS -Wall -Wextra -Wpedantic -Werror)

# rmk_module(<target> <source dir> [deps...]): a static library from every .cpp in <dir>.
function(rmk_module target dir)
    file(GLOB sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/*.cpp)
    if(NOT sources)
        return()
    endif()
    add_library(${target} STATIC ${sources})
    target_include_directories(${target} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_compile_options(${target} PRIVATE ${RMK_WARNINGS})
    target_link_libraries(${target} PUBLIC ${ARGN})
endfunction()

# rmk_program(<target> <source dir> [deps...]): an executable from every .cpp in <dir>.
function(rmk_program target dir)
    file(GLOB sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/*.cpp)
    if(NOT sources)
        return()
    endif()
    add_executable(${target} ${sources})
    target_compile_options(${target} PRIVATE ${RMK_WARNINGS})
    target_link_libraries(${target} PRIVATE ${ARGN})
    install(TARGETS ${target} RUNTIME DESTINATION bin)
endfunction()

# ----- Libraries

file(GLOB core_sources CONFIGURE_DEPENDS src/*.cpp src/stores/*.cpp)
add_library(rmk_core STATIC ${core_sources})
target_include_directories(rmk_core PUBLIC include)
target_compile_options(rmk_core PRIVATE ${RMK_WARNINGS})
target_link_libraries(rmk_core PUBLIC Threads::Threads)

rmk_module(rmk_ec src/ec rmk_core)

if(OpenSSL_FOUND)
    rmk_module(rmk_crypto src/crypto rmk_core OpenSSL::Crypto)
endif()

if(CURL_FOUND)
    rmk_module(rmk_http src/http rmk_core CURL::libcurl)
endif()

if(FDB_INCLUDE_DIR AND FDB_LIBRARY)
    rmk_module(rmk_fdb src/fdb rmk_core ${FDB_LIBRARY})
    if(TARGET rmk_fdb)
        target_include_directories(rmk_fdb PUBLIC ${FDB_INCLUDE_DIR})
        target_compile_definitions(rmk_fdb PUBLIC FDB_API_VERSION=730 RMK_HAVE_FDB=1)
    endif()
endif()

set(RMK_HAVE_RPC OFF)
if(gRPC_FOUND AND Protobuf_FOUND AND TARGET rmk_crypto
   AND TARGET rmk_ec AND TARGET rmk_http)
    set(RMK_HAVE_RPC ON)
endif()

if(RMK_HAVE_RPC)
    set(proto_dir ${CMAKE_CURRENT_SOURCE_DIR}/proto)
    set(gen_dir ${CMAKE_CURRENT_BINARY_DIR}/gen)
    file(MAKE_DIRECTORY ${gen_dir})
    get_target_property(grpc_plugin gRPC::grpc_cpp_plugin LOCATION)
    set(proto_sources)
    foreach(name storage datanode)
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

    # Status mapping, Data Node client (replicated writes, replica/EC reads), store factory.
    set(store_libs rmk_core)
    if(TARGET rmk_fdb)
        list(APPEND store_libs rmk_fdb)
    endif()
    rmk_module(rmk_rpc src/rpc rmk_proto rmk_core rmk_ec rmk_crypto rmk_http ${store_libs})

    rmk_module(rmk_datanode_lib src/datanode rmk_rpc rmk_http)
    rmk_program(rmk-datanode src/datanode/main rmk_datanode_lib)
    rmk_module(rmk_gateway_lib src/gateway rmk_rpc rmk_crypto rmk_http)
    rmk_program(rmk-gateway src/gateway/main rmk_gateway_lib)
    rmk_module(rmk_controller_lib src/controller rmk_rpc rmk_crypto rmk_http)
    rmk_program(rmk-controller src/controller/main rmk_controller_lib)
    # Everything in one process on in-memory stores: local development without FoundationDB.
    if(TARGET rmk_datanode_lib AND TARGET rmk_gateway_lib AND TARGET rmk_controller_lib)
        rmk_program(rmk-dev src/dev rmk_datanode_lib rmk_gateway_lib rmk_controller_lib)
    endif()

    # Client SDK: one public header, sdk/include/rmk/client.hpp.
    file(GLOB sdk_sources CONFIGURE_DEPENDS sdk/src/*.cpp)
    if(sdk_sources)
        add_library(rmk_client STATIC ${sdk_sources})
        target_include_directories(rmk_client PUBLIC sdk/include PRIVATE include)
        target_compile_options(rmk_client PRIVATE ${RMK_WARNINGS})
        target_link_libraries(rmk_client PRIVATE rmk_proto rmk_core)
        install(TARGETS rmk_client ARCHIVE DESTINATION lib)
        install(DIRECTORY sdk/include/ DESTINATION include)
    endif()
    rmk_program(rmkctl tools/rmkctl rmk_client)
endif()

# ----- Tests: one executable per folder under tests/, linked to the named libraries.

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

    function(rmk_tests name)
        foreach(dep ${ARGN})
            if(NOT TARGET ${dep})
                return()
            endif()
        endforeach()
        file(GLOB sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/${name}/*.cpp)
        if(NOT sources)
            return()
        endif()
        add_executable(rmk_${name}_tests ${sources})
        target_include_directories(rmk_${name}_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_compile_options(rmk_${name}_tests PRIVATE -Wall -Wextra -Werror)
        target_link_libraries(rmk_${name}_tests PRIVATE ${ARGN} GTest::gtest_main)
        gtest_discover_tests(rmk_${name}_tests DISCOVERY_TIMEOUT 30)
    endfunction()

    rmk_tests(core rmk_core)
    rmk_tests(stores rmk_core)
    rmk_tests(ec rmk_ec)
    rmk_tests(crypto rmk_crypto)
    rmk_tests(http rmk_http)
    rmk_tests(fdb rmk_fdb)
    rmk_tests(rpc rmk_rpc rmk_datanode_lib)
    rmk_tests(gateway rmk_gateway_lib rmk_datanode_lib)
    rmk_tests(controller rmk_controller_lib rmk_datanode_lib)
    rmk_tests(e2e rmk_client rmk_gateway_lib rmk_datanode_lib rmk_controller_lib)
endif()

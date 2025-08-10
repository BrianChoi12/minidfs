cmake_minimum_required(VERSION 3.16)
project(minidfs)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# === gRPC and Protobuf ===
find_package(Protobuf REQUIRED CONFIG)
find_package(gRPC REQUIRED CONFIG)

# === Proto definitions and generation ===
set(PROTO_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(GEN_SRC_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GEN_SRC_DIR})

set(PROTO_FILES
    ${PROTO_SRC_DIR}/dfs.proto
)

set(GENERATED_SRC)

foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)

    # Protobuf message generation
    add_custom_command(
        OUTPUT
            ${GEN_SRC_DIR}/${PROTO_NAME}.pb.cc
            ${GEN_SRC_DIR}/${PROTO_NAME}.pb.h
        COMMAND
            protobuf::protoc
        ARGS
            --cpp_out=${GEN_SRC_DIR}
            -I ${PROTO_SRC_DIR}
            ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
    )

    # gRPC service stub generation
    add_custom_command(
        OUTPUT
            ${GEN_SRC_DIR}/${PROTO_NAME}.grpc.pb.cc
            ${GEN_SRC_DIR}/${PROTO_NAME}.grpc.pb.h
        COMMAND
            protobuf::protoc
        ARGS
            --grpc_out=${GEN_SRC_DIR}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            -I ${PROTO_SRC_DIR}
            ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
    )

    list(APPEND GENERATED_SRC
        ${GEN_SRC_DIR}/${PROTO_NAME}.pb.cc
        ${GEN_SRC_DIR}/${PROTO_NAME}.pb.h
        ${GEN_SRC_DIR}/${PROTO_NAME}.grpc.pb.cc
        ${GEN_SRC_DIR}/${PROTO_NAME}.grpc.pb.h
    )
endforeach()

# === Source files ===
file(GLOB_RECURSE METASERVER_SRC metaserver/*.cpp)
file(GLOB_RECURSE DATANODE_SRC datanode/*.cpp)
file(GLOB_RECURSE CLIENT_SRC client/*.cpp)
file(GLOB_RECURSE COMMON_SRC common/*.cpp)

# === Libraries ===
set(GRPC_DEPS
    gRPC::grpc++
    protobuf::libprotobuf
)

# Find OpenSSL for checksums
find_package(OpenSSL REQUIRED)

# Find Google Test
find_package(GTest REQUIRED)
include(GoogleTest)

# === Targets ===
add_executable(minidfs_metaserver
    ${METASERVER_SRC}
    ${COMMON_SRC}
    ${GENERATED_SRC}
)
target_include_directories(minidfs_metaserver PRIVATE
    ${GEN_SRC_DIR}
    proto
    common
)
target_link_libraries(minidfs_metaserver PRIVATE ${GRPC_DEPS})

add_executable(minidfs_datanode
    ${DATANODE_SRC}
    ${COMMON_SRC}
    ${GENERATED_SRC}
)
target_include_directories(minidfs_datanode PRIVATE
    ${GEN_SRC_DIR}
    proto
    common
    datanode
)
target_link_libraries(minidfs_datanode PRIVATE ${GRPC_DEPS} OpenSSL::SSL OpenSSL::Crypto)

add_executable(minidfs_client
    ${CLIENT_SRC}
    ${COMMON_SRC}
    ${GENERATED_SRC}
)
target_include_directories(minidfs_client PRIVATE
    ${GEN_SRC_DIR}
    proto
    common
)
target_link_libraries(minidfs_client PRIVATE ${GRPC_DEPS})

# === Test Files ===
file(GLOB_RECURSE TEST_UTILS_SRC tests/utils/*.cpp)
file(GLOB_RECURSE UNIT_TEST_SRC tests/unit/*.cpp)
file(GLOB_RECURSE INTEGRATION_TEST_SRC tests/integration/*.cpp)
file(GLOB_RECURSE E2E_TEST_SRC tests/e2e/*.cpp)

# === Test Utilities Library ===
# Exclude main.cpp files from test utilities
list(REMOVE_ITEM METASERVER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/metaserver/main.cpp)
list(REMOVE_ITEM DATANODE_SRC ${CMAKE_CURRENT_SOURCE_DIR}/datanode/main.cpp)
list(REMOVE_ITEM CLIENT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/client/main.cpp)

add_library(minidfs_test_utils STATIC
    ${TEST_UTILS_SRC}
    ${METASERVER_SRC}
    ${CLIENT_SRC}
    ${DATANODE_SRC}
    ${GENERATED_SRC}
)
target_include_directories(minidfs_test_utils PUBLIC
    ${GEN_SRC_DIR}
    proto
    common
    metaserver
    datanode
    client
    tests/utils
)
target_link_libraries(minidfs_test_utils PUBLIC 
    ${GRPC_DEPS} 
    OpenSSL::SSL 
    OpenSSL::Crypto
    GTest::gtest
)

# === Unit Tests ===
add_executable(unit_tests
    ${UNIT_TEST_SRC}
    metaserver/cache.cpp
    datanode/storage.cpp
)
target_include_directories(unit_tests PRIVATE
    common
    metaserver
    datanode
    tests/unit
)
target_link_libraries(unit_tests PRIVATE 
    OpenSSL::SSL 
    OpenSSL::Crypto
    GTest::gtest_main
)

# === Integration Tests ===
add_executable(integration_tests
    ${INTEGRATION_TEST_SRC}
)
target_include_directories(integration_tests PRIVATE
    ${GEN_SRC_DIR}
    proto
    common
    metaserver
    datanode
    client
    tests/utils
)
target_link_libraries(integration_tests PRIVATE 
    minidfs_test_utils
    GTest::gtest_main
)

# === End-to-End Tests ===
add_executable(e2e_tests
    ${E2E_TEST_SRC}
)
target_include_directories(e2e_tests PRIVATE
    ${GEN_SRC_DIR}
    proto
    common
    metaserver
    datanode
    client
    tests/utils
)
target_link_libraries(e2e_tests PRIVATE 
    minidfs_test_utils
    GTest::gtest_main
)

# === Test Discovery ===
# Disabled due to timeout issues during test discovery phase
# gtest_discover_tests(unit_tests)
# gtest_discover_tests(integration_tests)
# gtest_discover_tests(e2e_tests)

# === Custom Test Targets ===
add_custom_target(test_all
    COMMAND ${CMAKE_CTEST_COMMAND} --verbose
    DEPENDS unit_tests integration_tests e2e_tests
    COMMENT "Running all MiniDFS tests"
)

add_custom_target(test_unit
    COMMAND unit_tests
    DEPENDS unit_tests
    COMMENT "Running unit tests"
)

add_custom_target(test_integration
    COMMAND integration_tests
    DEPENDS integration_tests
    COMMENT "Running integration tests"
)

add_custom_target(test_e2e
    COMMAND e2e_tests
    DEPENDS e2e_tests
    COMMENT "Running end-to-end tests"
)

add_custom_target(test_performance
    COMMAND e2e_tests --gtest_filter="PerformanceTest*"
    DEPENDS e2e_tests
    COMMENT "Running performance tests"
)

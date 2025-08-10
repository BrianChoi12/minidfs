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

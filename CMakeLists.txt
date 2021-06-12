cmake_minimum_required (VERSION 3.7.0)
project (lmdb-wrapper VERSION 0.1.0)


find_library(LMDB lmdb REQUIRED)

aux_source_directory (src SRC)
add_library (${PROJECT_NAME} ${SRC})

target_link_libraries (${PROJECT_NAME} PUBLIC ${LMDB})
if (WIN32)
    target_include_directories(${PROJECT_NAME} PUBLIC inc ${_VCPKG_ROOT_DIR}/installed/${VCPKG_TARGET_TRIPLET}/include)
else()
    target_include_directories (${PROJECT_NAME} PUBLIC inc)
endif ()
set_target_properties (${PROJECT_NAME} PROPERTIES CXX_STANDARD 17)
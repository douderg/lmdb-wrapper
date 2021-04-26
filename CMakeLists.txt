cmake_minimum_required (VERSION 3.7.0)
project (lmdb-wrapper VERSION 0.1.0)

find_library(LMDB lmdb REQUIRED)

set(CMAKE_CXX_STANDARD 17)

aux_source_directory (src SRC)
add_library (${PROJECT_NAME} ${SRC})

target_link_libraries(${PROJECT_NAME} ${LMDB})
target_include_directories (${PROJECT_NAME} PUBLIC inc)

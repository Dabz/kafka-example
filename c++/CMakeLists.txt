cmake_minimum_required (VERSION 2.8)

# projectname is the same as the main-executable
project(kafka-producer)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_definitions('-g')
add_definitions('-Wall')
add_definitions('-std=c++11')

file(GLOB SOURCES "src/*.cc")
include_directories("include")
include_directories("/usr/include")


add_executable(${PROJECT_NAME} ${SOURCES})
target_link_libraries(${PROJECT_NAME} LINK_PUBLIC -lrdkafka)

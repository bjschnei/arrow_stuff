
include(FetchContent)
FetchContent_Declare(
  sqlite3
  URL https://sqlite.org/2024/sqlite-amalgamation-3450300.zip
)

FetchContent_Populate(sqlite3)

#find_package(sqlite3 REQUIRED)

add_executable(translate translate.cc ${sqlite3_SOURCE_DIR}/sqlite3.c)
target_include_directories(translate
  PRIVATE
    $<BUILD_INTERFACE:${sqlite3_SOURCE_DIR}>)

target_link_libraries(translate PRIVATE 
                                     absl::strings
                                     Arrow::arrow_static 
                                     )

add_library(audit STATIC)
target_sources(audit
  PRIVATE
    audit.cc
    audit_cf_storage_helper.cc
    audit_syslog_storage_helper.cc)
target_include_directories(audit
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(audit
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    cql3)

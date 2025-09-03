add_library(neug_storages_csr OBJECT mutable_csr.cc)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_storages_csr>
        PARENT_SCOPE)
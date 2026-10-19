# Patches a protobuf-generated .pb.h so its namespace-scope default-instance
# variables carry the NEUG_API export annotation.
#
# protoc's --cpp_out=dllexport_decl=NEUG_API only annotates the generated
# class declarations. The default instances live at namespace scope
# (`extern ArrayDefaultTypeInternal _Array_default_instance_;`) and are left
# unannotated, so on Windows they never land in neug.dll's import library.
# Any target that includes the generated headers but links against neug.lib
# (python/node bindings) then fails to resolve those data symbols - the
# historical workaround copied the generated objects into every consumer,
# which duplicates the descriptor pools across modules and collides with the
# import library at link time (LNK2005).
#
# Usage (Windows only, invoked from the compile_proto custom command):
#   cmake -DPB_HEADER=<path to .pb.h> -P patch_pb_default_instances.cmake

if(NOT DEFINED PB_HEADER)
  message(FATAL_ERROR "PB_HEADER is not set")
endif()

file(READ "${PB_HEADER}" _content)
string(REGEX REPLACE
    "extern ([A-Za-z_][A-Za-z0-9_]*DefaultTypeInternal) (_[A-Za-z0-9_]+_default_instance_);"
    "extern NEUG_API \\1 \\2;"
    _content "${_content}")
file(WRITE "${PB_HEADER}" "${_content}")

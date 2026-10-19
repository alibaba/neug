# Shared patch-application helper for third_party trees.
#
# include_guard(GLOBAL): safe to include from multiple module scripts;
# the function definitions below stay available everywhere.

include_guard(GLOBAL)

# Apply <patch_file> inside <source_dir>, or detect that it is already
# applied. Uses git apply inside git checkouts (submodules) and the
# patch(1) executable elsewhere. Fails with the captured tool output so
# CI logs are self-diagnosing.
function(_neug_apply_patch source_dir patch_file patch_name)
    # git apply cannot parse a patch with CRLF line endings (an empty
    # context line becomes "\r", which neither ends the hunk nor counts as
    # context -> "corrupt patch at line N"). The .patch files live in the
    # main repo, so a core.autocrlf checkout on Windows leaves them CRLF.
    # CMake's own file I/O cannot fix this portably: file(READ) silently
    # strips CR, and file(WRITE) on Windows writes LF back as CRLF.
    # Detect CRLF byte-wise via a HEX read (which bypasses CMake's text
    # normalization) and fail with the normalization recipe instead.
    file(READ "${patch_file}" _neug_patch_hex HEX)
    string(FIND "${_neug_patch_hex}" "0d0a" _neug_patch_crlf)
    if(NOT _neug_patch_crlf EQUAL -1)
        message(FATAL_ERROR
            "${patch_name} has CRLF line endings, which git apply cannot "
            "parse (corrupt patch). A core.autocrlf checkout rewrites "
            "*.patch files to CRLF. Normalize them with:\n"
            "  git config --global core.autocrlf false\n"
            "  git config --global core.eol lf\n"
            "  git checkout HEAD -- .\n"
            "  git submodule update --init --force --recursive")
    endif()

    execute_process(
        COMMAND git rev-parse --show-toplevel
        WORKING_DIRECTORY "${source_dir}"
        RESULT_VARIABLE _git_check
        OUTPUT_VARIABLE _git_root
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
    file(REAL_PATH "${source_dir}" _source_real_path)
    if(_git_check EQUAL 0)
        file(REAL_PATH "${_git_root}" _git_real_path)
    endif()

    # --ignore-whitespace keeps the patches applicable on Windows when the
    # submodule sources (as opposed to the .patch file) are checked out
    # with CRLF; it is a no-op on LF-only checkouts (Linux/macOS).
    if(_git_check EQUAL 0 AND _git_real_path STREQUAL _source_real_path)
        set(_patch_check_command git apply --ignore-whitespace --check "${patch_file}")
        set(_patch_apply_command git apply --ignore-whitespace "${patch_file}")
        set(_patch_reverse_check_command
            git apply --reverse --ignore-whitespace --check "${patch_file}")
    else()
        find_program(_patch_executable patch REQUIRED)
        set(_patch_check_command
            "${_patch_executable}" -p1 -f --dry-run -i "${patch_file}")
        set(_patch_apply_command
            "${_patch_executable}" -p1 -f -i "${patch_file}")
        set(_patch_reverse_check_command
            "${_patch_executable}" -p1 -f -R --dry-run -i "${patch_file}")
    endif()

    execute_process(
        COMMAND ${_patch_check_command}
        WORKING_DIRECTORY "${source_dir}"
        RESULT_VARIABLE _patch_check
        ERROR_VARIABLE _patch_error)
    if(_patch_check EQUAL 0)
        execute_process(
            COMMAND ${_patch_apply_command}
            WORKING_DIRECTORY "${source_dir}"
            RESULT_VARIABLE _patch_result
            ERROR_VARIABLE _patch_error)
        if(NOT _patch_result EQUAL 0)
            message(FATAL_ERROR
                "Failed to apply ${patch_name}: ${_patch_error}")
        endif()
        message(STATUS "Applied ${patch_name}.")
        return()
    endif()

    execute_process(
        COMMAND ${_patch_reverse_check_command}
        WORKING_DIRECTORY "${source_dir}"
        RESULT_VARIABLE _patch_applied
        ERROR_VARIABLE _patch_reverse_error)
    if(_patch_applied EQUAL 0)
        message(STATUS "${patch_name} is already applied.")
    else()
        message(FATAL_ERROR
            "${patch_name} neither applies nor appears already applied. "
            "Apply error: ${_patch_error} "
            "Reverse-check error: ${_patch_reverse_error}")
    endif()
endfunction()

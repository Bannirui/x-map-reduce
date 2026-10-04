# Copies the shared asset directory next to the runtime binaries so examples
# can open assets by relative path when launched from build/bin.
#
# A single target is intentional: per-example targets race when they copy into
# the same destination in parallel.
function(add_asset_copy_target ASSET_SRC_DIR)
    if(TARGET copy-assets)
        return()
    endif()
    add_custom_target(copy-assets ALL
        COMMAND ${CMAKE_COMMAND} -E copy_directory
                "${ASSET_SRC_DIR}" "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/asset"
        COMMENT "Copying assets to ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/asset"
    )
endfunction()

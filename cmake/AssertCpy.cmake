function(add_asset_copy_target TARGET_NAME ASSET_SRC_DIR)
    set(COPY_NAME "${TARGET_NAME}-copy-assets")
    add_custom_target(${COPY_NAME} ALL
        COMMAND ${CMAKE_COMMAND} -E copy_directory "${ASSET_SRC_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/asset"
        COMMAND ${CMAKE_COMMAND} -E copy_directory "${ASSET_SRC_DIR}" "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/asset"
        COMMENT "Copying assets to build and runtime directories"
    )
    set(ASSET_COPY_TARGET "${COPY_NAME}" PARENT_SCOPE)
endfunction()
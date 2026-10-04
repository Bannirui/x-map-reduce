#pragma once

#include<cstddef>
#include<filesystem>
#include<string>

namespace mrapp {

// Map->Reduce intermediate file for map task `task`, reduce partition `partition`.
inline std::filesystem::path mapPartPath(const std::filesystem::path& workDir,
                                         std::size_t task,
                                         std::size_t partition){
    return workDir/("map-"+std::to_string(task)+"-part-"+std::to_string(partition)+".txt");
}

// A single reduce worker's output file.
inline std::filesystem::path reducePartPath(const std::filesystem::path& workDir,
                                            std::size_t partition){
    return workDir/("part-"+std::to_string(partition)+".txt");
}

}  // namespace mrapp

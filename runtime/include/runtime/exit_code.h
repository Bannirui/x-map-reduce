#pragma once

#include<stdexcept>

enum class ExitCode:int {
    Success = 0,
    Failure = 1 << 0,
    Usage = 1 << 1,
};

class UsageError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};
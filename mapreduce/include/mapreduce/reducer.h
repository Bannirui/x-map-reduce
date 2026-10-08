
#pragma once

#include"types.h"

#include<functional>
#include<vector>

using ReduceFunction=std::function<std::vector<KeyValue>(const Key&,const std::vector<Value>&)>;

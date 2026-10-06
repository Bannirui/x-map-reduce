#pragma once

#include"types.h"

#include<functional>
#include<vector>

using MapFunction=std::function<std::vector<KeyValue>(const Key&,const Value&)>;

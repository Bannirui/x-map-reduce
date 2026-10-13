#pragma once

#include"protocol/messages.h"
#include"runtime/task.h"

namespace xmr {
    protocol::WorkKind wireKind(TaskKind kind);

    TaskKind taskKind(protocol::WorkKind kind);

    Task toTask(const protocol::TaskAssignment& message);

    protocol::TaskAssignment toTaskAssignment(const Task& task);
} // namespace xmr

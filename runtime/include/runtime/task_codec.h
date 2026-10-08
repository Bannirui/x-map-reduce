#pragma once

#include"protocol/messages.h"
#include"runtime/task.h"

namespace xmr {
    protocol::WorkKind wireKind(TaskKind kind);

    TaskKind taskKind(protocol::WorkKind kind);

    Task toTask(const protocol::TaskMessage& message);

    protocol::TaskMessage toTaskMessage(const Task& task);
} // namespace xmr

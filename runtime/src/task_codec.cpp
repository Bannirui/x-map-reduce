#include"runtime/task_codec.h"

namespace xmr {
    protocol::WorkKind wireKind(TaskKind kind) {
        return kind == TaskKind::Map ? protocol::WorkKind::Map : protocol::WorkKind::Reduce;
    }

    TaskKind taskKind(protocol::WorkKind kind) {
        return kind == protocol::WorkKind::Map ? TaskKind::Map : TaskKind::Reduce;
    }

    Task toTask(const protocol::TaskAssignment& message) {
        Task task;
        task.kind = taskKind(message.kind);
        task.id = message.taskId;
        task.job = message.job;
        task.reducers = message.reducers;
        task.maps = message.maps;
        task.blocks = message.blocks;
        if (message.kind == protocol::WorkKind::Map) {
            task.input = *message.input;
        }
        return task;
    }

    protocol::TaskAssignment toTaskAssignment(const Task& task) {
        protocol::TaskAssignment message;
        message.kind = wireKind(task.kind);
        message.taskId = task.id;
        message.job = task.job;
        message.reducers = task.reducers;
        message.maps = task.maps;
        message.blocks = task.blocks;
        if (task.kind == TaskKind::Map) {
            message.input = task.input;
        }
        return message;
    }
} // namespace xmr

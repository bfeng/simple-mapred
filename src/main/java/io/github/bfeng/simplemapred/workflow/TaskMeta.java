package io.github.bfeng.simplemapred.workflow;

public class TaskMeta {
    public final TaskType type;
    public final int id;
    public final String host;
    public final int port;

    public TaskMeta(TaskType type, int id, String host, int port) {
        this.type = type;
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public enum TaskType {
        mapper,
        reducer
    }
}

package io.github.bfeng.simplemapred.resource;

import io.github.bfeng.simplemapred.workflow.TaskMeta;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Worker extends ServerBase {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    private final int workerId;
    private final int port;
    private int taskPort = 30000;

    private Map<Integer, Map<TaskMeta.TaskType, List<TaskMeta>>> taskMeta;

    public Worker(String configuration, int workerId) throws IOException {
        super(configuration);
        this.workerId = workerId;
        this.port = getWorkerConf().get(workerId).port;
        this.taskMeta = new HashMap<>();
    }

    public List<TaskMeta> getMapperConf(int clusterId) {
        return taskMeta.get(clusterId).get(TaskMeta.TaskType.mapper);
    }

    public List<TaskMeta> getReducerConf(int clusterId) {
        return taskMeta.get(clusterId).get(TaskMeta.TaskType.reducer);
    }

    private void addTaskMeta(int clusterId, TaskMeta meta) {
        Map<TaskMeta.TaskType, List<TaskMeta>> conf = taskMeta.computeIfAbsent(clusterId, k -> new HashMap<>());
        List<TaskMeta> list = conf.computeIfAbsent(meta.type, k -> new ArrayList<>());
        list.add(meta);
    }

    private TaskMeta startTask(TaskMeta.TaskType type, int taskId) {
        if (type == TaskMeta.TaskType.mapper)
            logger.info(String.format("Mapper[%d] started", taskId));
        else if (type == TaskMeta.TaskType.reducer)
            logger.info(String.format("Reducer[%d] started", taskId));
        return new TaskMeta(type, taskId, ":", taskPort++);
    }

    public void startMappers(int clusterId, List<Integer> mapperIds) {
        for (int id : mapperIds) {
            TaskMeta meta = startTask(TaskMeta.TaskType.mapper, id);
            addTaskMeta(clusterId, meta);
        }
    }

    public void startReducers(int clusterId, List<Integer> reducerIds) {
        for (int id : reducerIds) {
            TaskMeta meta = startTask(TaskMeta.TaskType.reducer, id);
            addTaskMeta(clusterId, meta);
        }
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port)
                .addService(new WorkerService(this))
                .build()
                .start();
        logger.info(String.format("Worker[%d] started, listening on %d", workerId, port));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                Worker.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final Worker server = new Worker(args[0], Integer.parseInt(args[1]));
        server.start();
        server.blockUntilShutdown();
    }

    static class WorkerService extends WorkerServiceGrpc.WorkerServiceImplBase {
        private final Worker worker;

        WorkerService(Worker worker) {
            this.worker = worker;
        }

        private List<TaskConf> convert(List<TaskMeta> metas) {
            List<TaskConf> configurations = new ArrayList<>();
            for (TaskMeta meta : metas) {
                TaskConf conf = TaskConf.newBuilder()
                        .setId(meta.id)
                        .setHost(meta.host)
                        .setPort(meta.port)
                        .build();
                configurations.add(conf);
            }
            return configurations;
        }

        @Override
        public void startMapper(StartMapperRequest request, StreamObserver<StartMapperResponse> responseObserver) {
            int clusterId = request.getClusterId();
            List<Integer> mapperIds = request.getMapperIdsList();
            worker.startMappers(clusterId, mapperIds);
            List<TaskMeta> metas = worker.getMapperConf(clusterId);
            List<TaskConf> configurations = convert(metas);
            StartMapperResponse response = StartMapperResponse.newBuilder()
                    .setClusterId(clusterId)
                    .addAllMappers(configurations)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void startReducer(StartReducerRequest request, StreamObserver<StartReducerResponse> responseObserver) {
            int clusterId = request.getClusterId();
            List<Integer> reducerIds = request.getReducerIdsList();
            worker.startReducers(clusterId, reducerIds);
            List<TaskMeta> metas = worker.getReducerConf(clusterId);
            List<TaskConf> configurations = convert(metas);
            StartReducerResponse response = StartReducerResponse.newBuilder()
                    .setClusterId(clusterId)
                    .addAllReducers(configurations)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}

package io.github.bfeng.simplemapred.resource;

import io.github.bfeng.simplemapred.workflow.TaskMeta;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Master extends ServerBase {
    private static final Logger logger = Logger.getLogger(Master.class.getName());

    private final int port;

    private Map<Integer, List<TaskMeta>> clusterMeta;

    public Master(String configuration) throws IOException {
        super(configuration);
        this.port = getMasterConf().port;
        this.clusterMeta = new HashMap<>();
    }

    public int newClusterId() {
        int id = clusterMeta.size();
        clusterMeta.put(id, new ArrayList<>());
        return id;
    }

    public void appendTaskMetas(int clusterId, List<TaskMeta> metas) {
        clusterMeta.get(clusterId).addAll(new ArrayList<>(metas));
    }

    public List<List<Integer>> roundRobin(int totalTasks, int totalWorkers) {
        List<List<Integer>> taskIds = new ArrayList<>(totalWorkers);
        for (int i = 0; i < totalWorkers; i++) {
            taskIds.add(new ArrayList<>());
        }

        int workerId = 0;
        for (int i = 0; i < totalTasks; i++) {
            taskIds.get(workerId).add(i);
            workerId++;
            if (workerId >= totalWorkers) {
                workerId = 0;
            }
        }
        return taskIds;
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port)
                .addService(new MasterService(this))
                .build()
                .start();
        logger.info("Master started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                Master.this.stop();
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
        final Master server = new Master(args[0]);
        server.start();
        server.blockUntilShutdown();
    }

    static class MasterService extends MasterServiceGrpc.MasterServiceImplBase {

        private final Master master;

        MasterService(Master master) {
            this.master = master;
        }

        private StartMapperRequest buildStartMapperRequest(int clusterId, List<Integer> mapperIds) {
            return StartMapperRequest.newBuilder()
                    .setClusterId(clusterId)
                    .addAllMapperIds(mapperIds)
                    .build();
        }

        private StartReducerRequest buildStartReducerRequest(int clusterId, List<Integer> reducerIds) {
            return StartReducerRequest.newBuilder()
                    .setClusterId(clusterId)
                    .addAllReducerIds(reducerIds)
                    .build();
        }

        private void startMappers(int clusterId, MachineConf workerConf, List<Integer> mapperIds) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(workerConf.ip, workerConf.port)
                    .usePlaintext()
                    .build();
            WorkerServiceGrpc.WorkerServiceBlockingStub stub
                    = WorkerServiceGrpc.newBlockingStub(channel);
            StartMapperResponse startMapperResponse =
                    stub.startMapper(buildStartMapperRequest(clusterId, mapperIds));
            List<TaskMeta> metas = startMapperResponse.getMappersList().stream().map((TaskConf conf) ->
                    new TaskMeta(TaskMeta.TaskType.mapper, conf.getId(), conf.getHost(), conf.getPort())
            ).collect(Collectors.toList());
            master.appendTaskMetas(clusterId, metas);
            channel.shutdown();
        }

        private void startReducers(int clusterId, MachineConf workerConf, List<Integer> reducerIds) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(workerConf.ip, workerConf.port)
                    .usePlaintext()
                    .build();
            WorkerServiceGrpc.WorkerServiceBlockingStub stub
                    = WorkerServiceGrpc.newBlockingStub(channel);
            StartReducerResponse startReducerResponse =
                    stub.startReducer(buildStartReducerRequest(clusterId, reducerIds));
            List<TaskMeta> metas = startReducerResponse.getReducersList().stream().map((TaskConf conf) ->
                    new TaskMeta(TaskMeta.TaskType.reducer, conf.getId(), conf.getHost(), conf.getPort())
            ).collect(Collectors.toList());
            master.appendTaskMetas(clusterId, metas);
            channel.shutdown();
        }

        private int startAll(int numberOfMappers, int numberOfReducers) {
            int clusterId = master.newClusterId();
            List<MachineConf> workerConfList = master.getWorkerConf();

            int totalWorkers = workerConfList.size();
            List<List<Integer>> mapperIds = master.roundRobin(numberOfMappers, totalWorkers);
            List<List<Integer>> reducerIds = master.roundRobin(numberOfReducers, totalWorkers);

            assert mapperIds.size() == totalWorkers;
            assert reducerIds.size() == totalWorkers;
            for (int i = 0; i < totalWorkers; i++) {
                logger.info(
                        String.format("Worker:%d, Mapper Ids:[%s]",
                                i,
                                mapperIds.get(i).stream()
                                        .map(String::valueOf)
                                        .collect(Collectors.joining(","))));
                logger.info(
                        String.format("Worker:%d, Reducer Ids:[%s]",
                                i,
                                reducerIds.get(i).stream()
                                        .map(String::valueOf)
                                        .collect(Collectors.joining(","))));
                startMappers(clusterId, workerConfList.get(i), mapperIds.get(i));
                startReducers(clusterId, workerConfList.get(i), reducerIds.get(i));
            }

            for (TaskMeta meta : master.clusterMeta.get(clusterId)) {
                if (meta.type == TaskMeta.TaskType.mapper)
                    logger.info(String.format("Cluster[%d]: Mapper task[%d] is running: %s:%d", clusterId, meta.id, meta.host, meta.port));
                else if (meta.type == TaskMeta.TaskType.reducer)
                    logger.info(String.format("Cluster[%d]: Reducer task[%d] is running: %s:%d", clusterId, meta.id, meta.host, meta.port));
            }

            return clusterId;
        }

        @Override
        public void initCluster(InitClusterRequest request, StreamObserver<InitClusterResponse> responseObserver) {
            int clusterId = startAll(request.getNumberOfMappers(), request.getNumberOfReducers());
            InitClusterResponse response = InitClusterResponse.newBuilder().setClusterId(clusterId).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void destroyCluster(DestroyClusterRequest request, StreamObserver<DestroyClusterResponse> responseObserver) {
            DestroyClusterResponse response = DestroyClusterResponse.newBuilder().setStatus(0).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
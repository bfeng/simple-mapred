package io.github.bfeng.simplemapred.resource;

import io.github.bfeng.simplemapred.workflow.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    public synchronized void appendTaskMetas(int clusterId, List<TaskMeta> metas) {
        clusterMeta.get(clusterId).addAll(new ArrayList<>(metas));
    }

    private List<TaskMeta> findTaskMetas(int clusterId, TaskMeta.TaskType taskType) {
        List<TaskMeta> taskMetas = new ArrayList<>();
        for (TaskMeta meta : clusterMeta.get(clusterId)) {
            if (meta.type == taskType) taskMetas.add(meta);
        }
        return taskMetas;
    }

    public List<TaskMeta> getMapperMetas(int clusterId) {
        return findTaskMetas(clusterId, TaskMeta.TaskType.mapper);
    }

    public List<TaskMeta> getReducerMetas(int clusterId) {
        return findTaskMetas(clusterId, TaskMeta.TaskType.reducer);
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

    private static class MasterService extends MasterServiceGrpc.MasterServiceImplBase {

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
                    stub.startMappers(buildStartMapperRequest(clusterId, mapperIds));
            List<TaskMeta> metas = startMapperResponse.getMappersList().stream().map((TaskConf conf) ->
                    new TaskMeta(TaskMeta.TaskType.mapper, conf.getId(), conf.getHost(), conf.getPort())
            ).collect(Collectors.toList());
            master.appendTaskMetas(clusterId, metas);
            channel.shutdown();
        }

        private void stopMappers(int clusterId, MachineConf workerConf) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(workerConf.ip, workerConf.port)
                    .usePlaintext()
                    .build();
            WorkerServiceGrpc.WorkerServiceBlockingStub stub = WorkerServiceGrpc.newBlockingStub(channel);

            StopMapperRequest request = StopMapperRequest.newBuilder().setClusterId(clusterId).build();
            StopMapperResponse response = stub.stopMappers(request);
            response.getStatusList().forEach(status -> logger.info(String.format("Mapper finished:%d", status)));
            channel.shutdown();
        }

        private void startReducers(int clusterId, MachineConf workerConf, List<Integer> reducerIds) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(workerConf.ip, workerConf.port)
                    .usePlaintext()
                    .build();
            WorkerServiceGrpc.WorkerServiceBlockingStub stub
                    = WorkerServiceGrpc.newBlockingStub(channel);
            StartReducerResponse startReducerResponse =
                    stub.startReducers(buildStartReducerRequest(clusterId, reducerIds));
            List<TaskMeta> metas = startReducerResponse.getReducersList().stream().map((TaskConf conf) ->
                    new TaskMeta(TaskMeta.TaskType.reducer, conf.getId(), conf.getHost(), conf.getPort())
            ).collect(Collectors.toList());
            master.appendTaskMetas(clusterId, metas);
            channel.shutdown();
        }

        private void stopReducers(int clusterId, MachineConf workerConf) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(workerConf.ip, workerConf.port)
                    .usePlaintext()
                    .build();
            WorkerServiceGrpc.WorkerServiceBlockingStub stub = WorkerServiceGrpc.newBlockingStub(channel);

            StopReducerRequest request = StopReducerRequest.newBuilder().setClusterId(clusterId).build();
            StopReducerResponse response = stub.stopReducers(request);
            response.getStatusList().forEach(status -> logger.info(String.format("Reducer finished:%d", status)));
            channel.shutdown();
        }

        private int startCluster(int numberOfMappers, int numberOfReducers) {
            int clusterId = master.newClusterId();
            List<MachineConf> workerConfList = master.getWorkerConf();
            int totalWorkers = workerConfList.size();
            List<List<Integer>> mapperIds = master.roundRobin(numberOfMappers, totalWorkers);
            List<List<Integer>> reducerIds = master.roundRobin(numberOfReducers, totalWorkers);

            assert mapperIds.size() == totalWorkers;
            assert reducerIds.size() == totalWorkers;
            ExecutorService THREAD_POOL = Executors.newFixedThreadPool(4);
            CompletionService<Integer> service = new ExecutorCompletionService<>(THREAD_POOL);
            for (int i = 0; i < totalWorkers; i++) {
                int finalI = i;
                service.submit(() -> {
                    startMappers(clusterId, workerConfList.get(finalI), mapperIds.get(finalI));
                    return 0;
                });
                service.submit(() -> {
                    startReducers(clusterId, workerConfList.get(finalI), reducerIds.get(finalI));
                    return 0;
                });
            }
            awaitTerminationAfterShutdown(THREAD_POOL);

            for (TaskMeta meta : master.clusterMeta.get(clusterId)) {
                if (meta.type == TaskMeta.TaskType.mapper)
                    logger.info(String.format("Cluster[%d]: Mapper[%d] is running: %s:%d", clusterId, meta.id, meta.host, meta.port));
                else if (meta.type == TaskMeta.TaskType.reducer)
                    logger.info(String.format("Cluster[%d]: Reducer[%d] is running: %s:%d", clusterId, meta.id, meta.host, meta.port));
            }

            return clusterId;
        }

        private void stopCluster(int clusterId) {
            List<MachineConf> workerConfList = master.getWorkerConf();
            int totalWorkers = workerConfList.size();
            ExecutorService THREAD_POOL = Executors.newFixedThreadPool(4);
            CompletionService<Integer> service = new ExecutorCompletionService<>(THREAD_POOL);
            for (int i = 0; i < totalWorkers; i++) {
                int finalI = i;
                service.submit(() -> {
                    stopMappers(clusterId, workerConfList.get(finalI));
                    return 0;
                });
                service.submit(() -> {
                    stopReducers(clusterId, workerConfList.get(finalI));
                    return 0;
                });
            }
            awaitTerminationAfterShutdown(THREAD_POOL);
        }

        private List<TaskConf> convert(List<TaskMeta> metas) {
            return metas.stream().map((TaskMeta meta) ->
                    TaskConf.newBuilder().setId(meta.id).setHost(meta.host).setPort(meta.port).build()
            ).collect(Collectors.toList());
        }

        private int runTask(int clusterId, String filePath, String className, TaskMeta taskMeta) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(taskMeta.host, taskMeta.port)
                    .usePlaintext()
                    .build();
            int status = 0;
            if (taskMeta.type == TaskMeta.TaskType.mapper) {
                MapperServiceGrpc.MapperServiceBlockingStub stub = MapperServiceGrpc.newBlockingStub(channel);
                RunMapperRequest request = RunMapperRequest.newBuilder()
                        .setInputFile(filePath)
                        .setMapClass(className)
                        .build();
                RunMapperResponse response = stub.runMapper(request);
                status = response.getStatus();
            } else if (taskMeta.type == TaskMeta.TaskType.reducer) {
                ReducerServiceGrpc.ReducerServiceBlockingStub stub = ReducerServiceGrpc.newBlockingStub(channel);
                int totalReducers = master.getReducerMetas(clusterId).size();
                List<TaskMeta> mapperMetas = master.getMapperMetas(clusterId);
                RunReducerRequest request = RunReducerRequest.newBuilder()
                        .setOutputFile(filePath)
                        .setReduceClass(className)
                        .setTotalReducers(totalReducers)
                        .addAllMappers(convert(mapperMetas))
                        .build();
                RunReducerResponse response = stub.runReducer(request);
                status = response.getStatus();
            }
            channel.shutdown();
            return status;
        }

        private void runMappers(int clusterId, List<String> filesList, String className) {
            if (filesList == null || filesList.size() == 0) return;
            for (String inputFile : filesList) {
                logger.info(String.format("Input: %s", inputFile));
            }
            List<TaskMeta> mapperTaskMetas = master.getMapperMetas(clusterId);
            int totalFiles = filesList.size();
            int totalMappers = mapperTaskMetas.size();
            List<List<Integer>> assignments = master.roundRobin(totalFiles, totalMappers);
            ExecutorService THREAD_POOL = Executors.newFixedThreadPool(4);
            CompletionService<Integer> service = new ExecutorCompletionService<>(THREAD_POOL);
            for (int i = 0; i < totalMappers; i++) {
                for (int j = 0; j < assignments.get(i).size(); j++) {
                    int finalI = i;
                    int finalJ = j;
                    service.submit(() -> {
                        String inputFile = filesList.get(assignments.get(finalI).get(finalJ));
                        return runTask(clusterId, inputFile, className, mapperTaskMetas.get(finalI));
                    });
                }
            }
            awaitTerminationAfterShutdown(THREAD_POOL);
        }

        private void runReducers(int clusterId, List<String> outputList, String className) {
            if (outputList == null || outputList.size() == 0) return;
            List<TaskMeta> reduceTaskMetas = master.getReducerMetas(clusterId);
            if (outputList.size() != reduceTaskMetas.size()) {
                logger.info("Output files should match reducers");
                return;
            }
            ExecutorService THREAD_POOL = Executors.newFixedThreadPool(4);
            CompletionService<Integer> service = new ExecutorCompletionService<>(THREAD_POOL);
            for (int i = 0; i < reduceTaskMetas.size(); i++) {
                int finalI = i;
                service.submit(() -> {
                    String outputFile = outputList.get(finalI);
                    logger.info("Output:  " + outputFile);
                    runTask(clusterId, outputFile, className, reduceTaskMetas.get(finalI));
                    return 0;
                });
            }
            awaitTerminationAfterShutdown(THREAD_POOL);
        }

        @Override
        public void initCluster(InitClusterRequest request, StreamObserver<InitClusterResponse> responseObserver) {
            int clusterId = startCluster(request.getNumberOfMappers(), request.getNumberOfReducers());
            InitClusterResponse response = InitClusterResponse.newBuilder().setClusterId(clusterId).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void runMapReduce(RunMapReduceRequest request, StreamObserver<RunMapReduceResponse> responseObserver) {
            int clusterId = request.getClusterId();
            List<String> filesList = request.getInputFilesList();
            List<String> outputList = request.getOutputFilesList();
            String className = request.getMapReduceClass();
            runMappers(clusterId, filesList, className);
            runReducers(clusterId, outputList, className);
            RunMapReduceResponse response = RunMapReduceResponse.newBuilder().setStatus(0).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void destroyCluster(DestroyClusterRequest request, StreamObserver<DestroyClusterResponse> responseObserver) {
            int clusterId = request.getClusterId();
            stopCluster(clusterId);
            DestroyClusterResponse response = DestroyClusterResponse.newBuilder().setStatus(0).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
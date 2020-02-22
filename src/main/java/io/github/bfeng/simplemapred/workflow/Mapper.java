package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Mapper extends TaskBase {
    private static final Logger logger = Logger.getLogger(Mapper.class.getName());

    private MapperEmitter<Message, Message> mapperEmitter = new MapperEmitter<>();

    protected Mapper(int id, String host, int port) {
        super(new TaskMeta(TaskMeta.TaskType.mapper, id, host, port));
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(meta.port)
                .addService(new MapperService(this))
                .build()
                .start();
        logger.info(String.format("Mapper[%d] started, listening on %d", meta.id, meta.port));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** Mapper shut down");
        }));
    }

    public int runMapperFn(String inputFile, String mapClass) {
        logger.info(mapClass + " runs input: " + inputFile);
        ReflectionUtils.runMapFn(mapClass, inputFile, mapperEmitter);
        return 0;
    }

    public static void main(String[] args) {
        logger.info("Mapper task: " + Arrays.deepToString(args));
        int id = Integer.parseInt(args[0]);
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        try {
            final Mapper server = new Mapper(id, host, port);
            server.start();
            server.blockUntilShutdown();
        } catch (IOException | InterruptedException e) {
            logger.fine(e.getMessage());
        }
    }

    private static class MapperService extends MapperServiceGrpc.MapperServiceImplBase {
        private final Mapper mapper;

        public MapperService(Mapper mapper) {
            this.mapper = mapper;
        }

        private Iterable<? extends Any> packList(List<Message> input) {
            return input.stream().map(Any::pack).collect(Collectors.toList());
        }

        @Override
        public void runMapper(RunMapperRequest request, StreamObserver<RunMapperResponse> responseObserver) {
            int status = 0;
            try {
                String inputFile = request.getInputFile();
                String className = request.getMapClass();
                status = mapper.runMapperFn(inputFile, className);
            } catch (Exception e) {
                logger.info(e.getMessage());
                status = -1;
            } finally {
                RunMapperResponse response = RunMapperResponse.newBuilder().setStatus(status).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }

        @Override
        public void stopMapper(StopLocalMapperRequest request, StreamObserver<StopLocalMapperResponse> responseObserver) {
            try {
                mapper.stop();
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            } finally {
                StopLocalMapperResponse response = StopLocalMapperResponse.newBuilder().setStatus(0).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }

        @Override
        public void readCombinedKeyValues(ReadKeyValueRequest request, StreamObserver<CombinedKeyValuePairs> responseObserver) {
            int reduceId = request.getReduceId();
            int totalReducer = request.getTotalReducers();
            logger.info(String.format("Mapper[%d] is sending data to Reducer[%d]", mapper.meta.id, reduceId));
            int counter = 0;
            for (Message key : mapper.mapperEmitter.getKeys()) {
                logger.info(key + "hashcode:" + key.hashCode());
                if ((Math.abs(key.hashCode()) % totalReducer) == reduceId) {
                    CombinedKeyValuePairs response = CombinedKeyValuePairs.newBuilder()
                            .setKey(Any.pack(key))
                            .addAllValues(packList(mapper.mapperEmitter.getList(key)))
                            .build();
                    responseObserver.onNext(response);
                    counter++;
                }
            }
            logger.info(
                    String.format("Mapper[%d] is sending data to Reducer[%d] (%d keys sent / total %d)",
                            mapper.meta.id, reduceId, counter, mapper.mapperEmitter.getKeys().size())
            );
            responseObserver.onCompleted();
        }
    }
}

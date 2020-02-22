package io.github.bfeng.simplemapred.app;

import io.github.bfeng.simplemapred.resource.InitClusterRequest;
import io.github.bfeng.simplemapred.resource.RunMapReduceRequest;
import io.github.bfeng.simplemapred.workflow.GenericMapReduce;
import io.github.bfeng.simplemapred.workflow.MapperEmitter;
import io.github.bfeng.simplemapred.workflow.ReducerEmitter;
import io.github.bfeng.simplemapred.workflow.types.TextMsg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class InvertedIndex extends SimpleMapReduce {

    public static class MapReduce implements GenericMapReduce<TextMsg, TextMsg, TextMsg, TextMsg> {

        @Override
        public void map(String inputFile, MapperEmitter<TextMsg, TextMsg> emitter) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(inputFile));
                TextMsg outValue = TextMsg.newBuilder().setContent(inputFile).build();
                String line = null;
                while ((line = reader.readLine()) != null) {
                    StringTokenizer st = new StringTokenizer(line);
                    while (st.hasMoreTokens()) {
                        String key = st.nextToken();
                        TextMsg outKey = TextMsg.newBuilder().setContent(key).build();
                        emitter.write(outKey, outValue);
                    }
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(TextMsg word, Iterable<TextMsg> listOfFiles, ReducerEmitter<TextMsg, TextMsg> emitter) {
            Set<TextMsg> names = new HashSet<>();
            for (TextMsg file : listOfFiles) {
                names.add(file);
            }
            for (TextMsg file : names) {
                if (emitter.contains(word)) {
                    List<TextMsg> values = emitter.getValues(word);
                    if (!values.contains(file)) {
                        emitter.write(word, file);
                    }
                } else
                    emitter.write(word, file);
            }
        }
    }

    @Override
    protected InitClusterRequest buildInitRequest() {
        return InitClusterRequest
                .newBuilder()
                .setNumberOfMappers(2)
                .setNumberOfReducers(2)
                .build();
    }

    @Override
    protected RunMapReduceRequest buildRunMapReduceRequest() {
        List<String> inputFiles = Arrays.asList("input/part-1.txt", "input/part-2.txt");
        List<String> outputFiles = Arrays.asList("output/inverted-1.txt", "output/inverted-2.txt");
        return RunMapReduceRequest.newBuilder()
                .setClusterId(clusterId)
                .addAllInputFiles(inputFiles)
                .addAllOutputFiles(outputFiles)
                .setMapReduceClass(MapReduce.class.getName())
                .build();
    }

    public static void main(String[] args) {
        InvertedIndex app = new InvertedIndex();
        String masterIP = "localhost";
        int port = 12345;
        app.initCluster(masterIP, port);
        app.runMapReduce(masterIP, port);
        app.destroyCluster(masterIP, port);
    }
}

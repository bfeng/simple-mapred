package io.github.bfeng.simplemapred.app;

import io.github.bfeng.simplemapred.workflow.GenericMapReduce;
import io.github.bfeng.simplemapred.workflow.MapperEmitter;
import io.github.bfeng.simplemapred.workflow.ReducerEmitter;
import io.github.bfeng.simplemapred.workflow.types.TextMsg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

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

    public static void main(String[] args) {
        InvertedIndex app = new InvertedIndex();
        app.parseArgs(args);
        String masterIP = app.masterHost;
        int port = app.masterPort;
        app.initCluster(masterIP, port);
        app.runMapReduce(masterIP, port, MapReduce.class.getName());
        app.destroyCluster(masterIP, port);
    }
}

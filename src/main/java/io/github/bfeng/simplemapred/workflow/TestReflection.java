package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.github.bfeng.simplemapred.app.WordCountApp;

import java.util.Arrays;
import java.util.List;

public class TestReflection {
    public static void main(String[] args) {
        String className = WordCountApp.MapReduce.class.getName();
        runMapReduce(className, Arrays.asList("input/words-1.txt", "input/words-2.txt"));
    }

    public static void runMapReduce(String className,
                                    List<String> inputFiles) {
        MapperEmitter<Message, Message> mapperEmitter = new MapperEmitter<>();
        ReducerEmitter<Message, Message> reducerEmitter = new ReducerEmitter<>();
        for (String inputFile : inputFiles) {
            ReflectionUtils.runMapFn(className, inputFile, mapperEmitter);
        }
        for (Message key : mapperEmitter.getKeys()) {
            System.out.println(key + "hashcode:" + key.hashCode());
            System.out.println(String.format("%d %% %d = %d", key.hashCode(), 2, key.hashCode() % 2));
            List<Any> values = ReflectionUtils.packList(mapperEmitter.getList(key));
            ReflectionUtils.runReduceFn(className, Any.pack(key), values, reducerEmitter);
        }
    }
}

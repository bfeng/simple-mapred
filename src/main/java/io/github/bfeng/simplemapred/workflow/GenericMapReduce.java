package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Message;

public interface GenericMapReduce<
        MapperOutKey extends Message,
        MapperOutValue extends Message,
        ReducerOutKey extends Message,
        ReducerOutValue extends Message> {

    void map(String inputFile, MapperEmitter<MapperOutKey, MapperOutValue> emitter);

    void reduce(MapperOutKey key, Iterable<MapperOutValue> values, ReducerEmitter<ReducerOutKey, ReducerOutValue> emitter);
}

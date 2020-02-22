package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Message;

import java.util.*;

public class MapperEmitter<MapperOutKey extends Message, MapperOutValue extends Message> {
    Map<MapperOutKey, List<MapperOutValue>> data;

    public MapperEmitter() {
        data = new HashMap<>();
    }

    public void collect(MapperOutKey outKey, MapperOutValue outValue) {
        List<MapperOutValue> list = data.computeIfAbsent(outKey, k -> new ArrayList<>());
        list.add(outValue);
    }

    public Set<MapperOutKey> getKeys() {
        return data.keySet();
    }

    public List<MapperOutValue> getList(MapperOutKey key) {
        return data.get(key);
    }
}

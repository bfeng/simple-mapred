package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Message;

import java.util.*;

public class MapperEmitter<MapperOutKey extends Message, MapperOutValue extends Message> {
    Map<MapperOutKey, List<MapperOutValue>> data;

    public MapperEmitter() {
        data = new HashMap<>();
    }

    private void collect(MapperOutKey outKey, MapperOutValue outValue) {
        List<MapperOutValue> list = data.computeIfAbsent(outKey, k -> new ArrayList<>());
        list.add(outValue);
    }

    public Set<MapperOutKey> getKeys() {
        return data.keySet();
    }

    public List<MapperOutValue> getList(MapperOutKey key) {
        return data.get(key);
    }

    public void write(MapperOutKey outKey, MapperOutValue outValue) {
        System.out.println(String.format("Mapper out: key={%s},value={%s}", outKey, outValue));
        collect(outKey, outValue);
    }
}

package master2018;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;

public class StreamTuple {
    public static class StreamTuple implements DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
        
    }
}

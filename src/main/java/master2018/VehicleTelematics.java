package master2018;

import com.google.protobuf.ByteString;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.logging.Filter;

public class VehicleTelematics {

    public static void main(String[] args){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = args[0];
        String outFilePath = args[1];
        DataStreamSource<String> source = env.readTextFile(inFilePath);
        DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream = source
                .map(new StreamParser());
        //SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> speedFines =
        //        dataStream.filter(new SpeedFines());
        //speedFines.writeAsCsv(outFilePath + "speedFines.csv").setParallelism(1);

/*        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> averageSpeedControl =
                dataStream.filter(new SegmentFilter());
        averageSpeedControl.keyBy(1);*/

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> accidentControl = dataStream
                .keyBy(1).window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(30)))
                .apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Object, Tuple, TimeWindow>() {
                    public void static <Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Object, Tuple, TimeWindow> {
                        return null;

                    }
                });
        dataStream.writeAsCsv(outFilePath + "accidentControl.csv").setParallelism(1);

        try {
            env.execute("VehicleTelematics");
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static final class StreamParser implements MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>{
        @Override
        public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String line) throws Exception {
            String[] cells = line.split(",");
            return new Tuple8<>(Integer.parseInt(cells[0]),Integer.parseInt(cells[1]), Integer.parseInt(cells[2]),
                    Integer.parseInt(cells[3]), Integer.parseInt(cells[4]), Integer.parseInt(cells[5]),
                    Integer.parseInt(cells[6]), Integer.parseInt(cells[7]));
        }
    }

    public static final class SpeedFines implements FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> inStream) throws Exception{
            return inStream.f2 > 90;
        }
    }

    public static final class SegmentFilter implements FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> inStream) throws Exception {
            return inStream.f6 >= 52 && inStream.f6 <= 56;
        }
    }

}

package master2018;

import com.google.protobuf.ByteString;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Filter;

public class VehicleTelematics {

    public static void main(String[] args){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = args[0];
        String outFilePath = args[1];
        DataStreamSource<String> source = env.readTextFile(inFilePath);
   /*     Long[] VehicleInfoValues = source.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String line) throws Exception {
                String[] cells = line.split(",");
                Long[] values = new Long[cells.length];
                for (int i = 0; i < cells.length; i++) {
                    values[i] = Long.parseLong(cells[i]);
                }
                return values;
            }
        });*/
        VehicleInfo vehicleInfo = new VehicleInfo();

        DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream = source
                .map(new StreamParser());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                return element.f0*1000;
            }
        });

        //maxSpeedFines(dataStream, outFilePath);

        //Collector<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> collector = dataStream.windoww

        try {
            env.execute("VehicleTelematics");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
/*    public static class SimpleSum implements
            WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple8<Integer,
                    Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple key, TimeWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {

            Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
            Integer id = 0;
            Long ts = 0L;
            Double temp = 0.0;
            if(first != null){
                id=first.f1;
                ts=first.f0;
                temp=first.f2;
            }
            while(iterator.hasNext()){
                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                ts=next.f0;
                temp += next.f2;
            }
            out.collect(new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(ts, id, temp));
        }
    }*/

    public static void maxSpeedFines(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream,
                              String outFilePath){
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> speedFines =
                dataStream.filter(new SpeedFines());

        //speedFines.writeAsCsv(outFilePath + "speedFines.csv").setParallelism(1);
        //speedFines.writeAsText(String.format("%s, %s, %s, %s, %s, %s", speedFines.))
    }

    public static void detectAccidents(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream,
                                         String outFilePath){
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> stationaryVehicles =
                dataStream.filter(new StationaryVehicle());
        stationaryVehicles.keyBy(1).keyBy(2).countWindow(4, 1);
        stationaryVehicles.writeAsCsv(outFilePath + "stationaryVehicles.csv").setParallelism(1);

    }
    public static void averageSpeedFines(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream,
                                         String outFilePath) {
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> averageSpeedControl =
                dataStream.filter(new SegmentFilter());
        averageSpeedControl.keyBy(1);
        dataStream.writeAsCsv(outFilePath + "accidentControl.csv").setParallelism(1);

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
    public static final class StationaryVehicle implements FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> inStream) throws Exception {
            return inStream.f2 == 0;
        }
    }


}



/*

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> accidentControl = dataStream
                .keyBy(1).window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(30)))
                .apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Object, Tuple, TimeWindow>() {
                    public void static <Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Object, Tuple, TimeWindow> {
                        return null;

                    }
                });
*/

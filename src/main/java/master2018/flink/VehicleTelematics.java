package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;

import java.util.HashSet;
import java.util.Iterator;

public class VehicleTelematics {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String inFilePath = args[0];
        String outFilePath = args[1];

        DataStreamSource<String> source = env.readTextFile(inFilePath).setParallelism(1);

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleRecords = source.map(new StreamParser()).setParallelism(1);


        maxSpeedFines(vehicleRecords, outFilePath);
        averageSpeedFines(vehicleRecords, outFilePath);
        detectAccidents(vehicleRecords, outFilePath);

        env.execute("VehicleTelematics");
    }


    public static void maxSpeedFines(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream,
                                     String outFilePath){
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> speedFines =
                dataStream.filter(new SpeedFines()).setParallelism(1);
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> outputStreamOperator = speedFines.project(0, 1, 3, 6, 5, 2);
        outputStreamOperator.writeAsCsv(outFilePath + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }


        public static void detectAccidents(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream,
                                             String outFilePath){                       //timestamp
            SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> stationaryVehicles =
                    dataStream.filter(new StationaryVehicle()).setParallelism(1);
            stationaryVehicles.keyBy(1)
                    .countWindow(4, 1)
                    .apply(VehicleTelematics::getEventTimestamps)
                    .writeAsCsv(outFilePath + "accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }



    private static void getEventTimestamps(Tuple tuple, GlobalWindow globalWindow, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> tuple8s,
                                    Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) {
        if (Iterables.size(tuple8s) == 4) {
            Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = tuple8s.iterator();
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> startEvent = null, lastEvent = null, temp = null;
            int counter = 1;
            while (iterator.hasNext()) {
                temp = iterator.next();
                if (counter == 1 || (startEvent!=null && temp.f0 < startEvent.f0))
                    startEvent = temp;
                 else if(counter == 4 || (lastEvent != null && temp.f0 > lastEvent.f0))
                    lastEvent = temp;
                counter++;
                if (startEvent != null && lastEvent != null && startEvent.f0 == lastEvent.f0 - 90) {
                    out.collect(new Tuple7<>(startEvent.f0, lastEvent.f0,
                            lastEvent.f1, lastEvent.f3, lastEvent.f6,
                            lastEvent.f5, lastEvent.f7));
                    startEvent = null;
                    lastEvent = null;
                }
            }
        }
    }

    public static void averageSpeedFines(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream,
                                         String outFilePath) {

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> averageSpeedControl =
                dataStream.filter(new SegmentFilter());
        averageSpeedControl.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                return element.f0*1000;
            }
        })
                .keyBy(1,3,5)
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out) throws Exception {
                        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> seg52 = null, seg56 = null; //Register the beginning and end of the segments.
                        double averageSpeed=0.0;
                        int totalRecords = Iterables.size(input); // Total records that exist in the current window
                        HashSet<Integer> distinctSeg = new HashSet<>(); // Here we register all the distinct segments where the vehicle goes through.
                        int startTime=Integer.MAX_VALUE;
                        int endTime=Integer.MIN_VALUE;

                        //Wee need to have more that 4 records for each window
                        if (totalRecords < 4)
                            return;


                        for (Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> record : input) {

                            distinctSeg.add(record.f6); //Register the current segment and add it to the hashset if it doesnt exist.


                            if (record.f6.equals(52)) {					//checking for segment 52
                                if (seg52 != null) {
                                    if(record.f5.equals(0)){				//considering  direction 0
                                        if (record.f0 < seg52.f0)		//if timestamp of record is less than timestamp of segment 52
                                            seg52 = record;
                                    }else if(record.f5.equals(1))		//considering direction 1
                                        if(record.f0 > seg52.f0)
                                            seg52 = record;
                                }else
                                    seg52 = record;						//transfering record to segment 52
                            }else if (record.f6.equals(56)) {			//checking for segment 56
                                if(seg56 != null){
                                    if(record.f5.equals(0)){				//considering direction 0
                                        if(record.f0 > seg56.f0)			//comparing timestamp of record with segment 56
                                            seg56=record;
                                    }else if(record.f5.equals(1)){		//considering direction 1
                                        if(record.f0 < seg56.f0)			//comparing timestamp of record with segment 56
                                            seg56=record;
                                    }
                                }else{
                                    seg56=record;
                                }
                            }
                            averageSpeed += record.f2;						// calculating average speed of cars in segment
                        }
                        averageSpeed /= totalRecords;

                        if (distinctSeg.size() == 5 && averageSpeed >= 60 && seg52 != null && seg56 != null) {

                            if(seg52.f0<seg56.f0){
                                startTime = seg52.f0;
                            }
                            else {
                                startTime = seg56.f0;
                            }

                            if(seg52.f0<seg56.f0) {
                                endTime = seg56.f0;
                            }
                            else {
                                endTime = seg52.f0;
                            }

                            out.collect(new Tuple6<>(startTime, endTime, seg52.f1, seg52.f3, seg52.f5, averageSpeed));
                        }
                    }
                })

                .writeAsCsv(outFilePath + "/avgspeedfines.csv",FileSystem.WriteMode.OVERWRITE).setParallelism(1);

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
        public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> inStream) throws Exception {
            return inStream.f2 >90;
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


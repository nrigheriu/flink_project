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
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Filter;

public class VehicleTelematics {

    public static void main(String[] args) throws Exception {
    	
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        String inFilePath = args[0];
        String outFilePath = args[1];
        
        DataStreamSource<String> source = env.readTextFile(inFilePath).setParallelism(1);
        
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleRecords = source.map(new StreamParser()).setParallelism(1); 
        
        //vehicleRecords
        	//.map(new maxSpeedFines());
        
        
    
        
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
        
        
        //VehicleInfo vehicleInfo = new VehicleInfo();
        
//parsing the vehicle record
  /*    
        DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream = source
                .map(new StreamParser());
        
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                return element.f0*1000;
            }
        });
        */
        
     
        // trying for speed fine
       
       maxSpeedFines(vehicleRecords, outFilePath);
       averageSpeedFines(vehicleRecords, outFilePath);
       
      
        //Collector<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> collector = dataStream.windoww

       // try {
        System.out.println("going for env.execute-----------");
            env.execute("VehicleTelematics");
       // } catch (Exception e){
         //   e.printStackTrace();
      //  }
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
    	System.out.println("inside maxspeedfine");
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> speedFines =
                dataStream.filter(new SpeedFines());
        speedFines.writeAsCsv(outFilePath + "/speedFines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }
        
   
/*
    public static void detectAccidents(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream,
                                         String outFilePath){
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> stationaryVehicles =
                dataStream.filter(new StationaryVehicle());
        stationaryVehicles.keyBy(1).keyBy(2).countWindow(4, 1);
        stationaryVehicles.writeAsCsv(outFilePath + "stationaryVehicles.csv").setParallelism(1);

    }*/
    public static void averageSpeedFines(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream,
                                         String outFilePath) {
    	System.out.println("inside average speed fine");
    
    	
    	
    	
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
                 
                   if(seg52.f0<seg56.f0)
                   {
                	   startTime = seg52.f0;
                   }
                   else {
                	   startTime = seg56.f0;
                   }
                   
                 
                   if(seg52.f0<seg56.f0)
                   {
                	   endTime = seg56.f0;
                   }
                   else
                   {
                	   endTime = seg52.f0;
                   }

                   out.collect(new Tuple6<>(startTime, endTime, seg52.f1, seg52.f3, seg52.f5, averageSpeed));
               }
           }
       })
        
        .writeAsCsv(outFilePath + "/AverageSpeedFine.csv",FileSystem.WriteMode.OVERWRITE).setParallelism(1);

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



/*

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> accidentControl = dataStream
                .keyBy(1).window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(30)))
                .apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Object, Tuple, TimeWindow>() {
                    public void static <Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Object, Tuple, TimeWindow> {
                        return null;

                    }
                });
*/

package com.qzl.window;

import com.qzl.vo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.Calendar;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //linux nc -lk 6666  开启socket 服务
        DataStream d=env.socketTextStream("10.122.1.87",6666);
        SingleOutputStreamOperator<Event> ed=d.map(new MapFunction<String, Event>() {

            @Override
            public Event map(String s) throws Exception {
                try{
                    String[] es=s.split(" ");
                    Event e=new Event(es[0],es[1], Calendar.getInstance().getTimeInMillis());
                    return e;
                }catch (Exception e){
                    e.printStackTrace();
                }
                return null;
            }
        }).filter(a->a!=null)
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));

        ed.keyBy(aa->aa.user).print();




        env.execute();

    }
}

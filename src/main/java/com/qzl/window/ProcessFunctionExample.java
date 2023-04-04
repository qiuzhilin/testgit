package com.qzl.window;

import com.qzl.source.ClickSource;
import com.qzl.vo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTimestamp();
                                    }
                                }
                        )
                ).process(new ProcessFunction<Event, Object>() {
            @Override
            public void processElement(Event value, Context context, Collector<Object> out) throws Exception {
                if (value.user.equals("Mary")) { out.collect(value.user);
                } else if (value.user.equals("Bob")) { out.collect(value.user); out.collect(value.user);
                }
                System.out.println(context.timerService().currentWatermark());

            }
        }).print();
        env.execute();
    }
}

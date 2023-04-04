package com.qzl.wc;

import com.qzl.vo.Discount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

public class SideOutSteam {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //linux nc -lk 6666  开启socket 服务
        DataStream inputDataStream=env.socketTextStream("10.122.1.87",6666);

        // 定义广播配置流
        MapStateDescriptor<String, String> configFilter = new MapStateDescriptor<String, String>("configFilter", BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO);
        BroadcastStream<String> broadcastConfig = env.socketTextStream("10.122.1.87",7777)
                .broadcast(configFilter);

        //对广播数据进行关联
        inputDataStream.connect(broadcastConfig)
                .process(new BroadcastProcessFunction<String,String,String>()  {


                    Map<String,String> mapState=new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState.put("aa","aa");



                        getRuntimeContext();
                        //ReadOnlyBroadcastState<String, String> bcstate= getRuntimeContext().getb.getBroadcastState(configFilter);
                       // super.open(parameters);
                    }

                    @Override
            public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
               // BroadcastState<String, String> bcState = readOnlyContext.getBroadcastState(configFilter);
                ReadOnlyBroadcastState<String, String> bcstate= readOnlyContext.getBroadcastState(configFilter);
                System.out.println("数据流："+ s);
                if(mapState.containsKey(s)){
                    System.out.println("存在广播状态中");
                    collector.collect(s);
                }

            }

            @Override
            public void processBroadcastElement(String s, Context context, Collector<String> collector) throws Exception {
                System.out.println("broadcast 收到：+"+s);
                BroadcastState<String, String> bcState = context.getBroadcastState(configFilter);
                bcState.put(s,s);
                mapState.put(s,s);
            }
        }).print();


//        OutputTag<String> sideOutputTag = new OutputTag<String>("side-output"){};
//        OutputTag<String> sideInfo = new OutputTag<String>("side-info"){};
//
//        SingleOutputStreamOperator<String> result = inputDataStream
//                .process(new ProcessFunction<String, String>() {
//                    @Override
//                    public void processElement(String input, Context context, Collector<String> collector) throws Exception {
//                        if (input.startsWith("ERROR")) {
//                            context.output(sideOutputTag, "Error: " + input);
//                        } else if(input.startsWith("INFO")){
//                            context.output(sideInfo, "Info: " + input);
//                        }
//                        else {
//                            collector.collect(input);
//                        }
//                    }
//                });
//        DataStream<String> sideOutputStream = result.getSideOutput(sideOutputTag);
//        sideOutputStream.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                return s+"0000";
//            }
//        }).print();
//
//        DataStream<String> infoStream = result.getSideOutput(sideInfo);
//        infoStream.print();
//        sideOutputStream.print();
        env.execute();

    }
}

package com.qzl.source;

import com.qzl.vo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    //定义一个标识
    private boolean sendContinue=true;
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        //定义两组数据
        String[] names={"Mary", "Alice", "Bob", "Cary"};
        String[] urls={"./home",	"./cart",	"./fav",	"./prod?id=1","./prod?id=2"};
        //定义一个随机器
        Random random=new Random();
        while (sendContinue){
            Event event=new Event(names[random.nextInt(names.length)],urls[random.nextInt(urls.length)]
                    , Calendar.getInstance().getTimeInMillis());
            sourceContext.collect(event);
            System.out.println(event.toString());
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        sendContinue=false;
    }
}

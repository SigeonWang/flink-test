package com.wxj.steaming.window;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/30 22:24
 * @Description: TODO
 */
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction());
        // 使用lambda表达式只调用一个已经存在的方法且不做其它操作时，可以使用双冒号"::"的形式，称为"方法引用"
        KeyedStream<WaterSensor, String> keyDs = ds.keyBy(WaterSensor::getId);

        // 窗口分配器：基于 计数 的 窗口
        // 1）滚动窗口
        WindowedStream<WaterSensor, String, GlobalWindow> countWs = keyDs.countWindow(5);
        // 2）滑动窗口
        // 注意：对于滑动窗口，每经过一个步长，一定有一个窗口触发（基于时间的也类似）！
        //     假设窗口长度5，步长2，数据是1,2,3,4,5,6,7...：
        //     对第一个窗口，输出1,2；第二个窗口输出1,2,3,4；第二个窗口输出2,3,4,5,6；以此类推
//        WindowedStream<WaterSensor, String, GlobalWindow> countWs = keyDs.countWindow(5, 2);
        // 3）基于 计数 的 全局窗口。
        // 计数窗口的底层调用就是GlobalWindows，需要自定义触发器，很少使用。
//        keyDs.window(GlobalWindows.create());

        // 窗口函数：全窗口函数 provess
        countWs.process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
            @Override
            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long maxTs = context.window().maxTimestamp();
                long cnt = elements.spliterator().estimateSize();  // 数据的估计条数
                out.collect("key="+s+"的窗口最大时间戳" + maxTs +
                        ", 包含 " + cnt + " 条数据 => " + elements.toString());
            }
        }).print();

        env.execute();
    }
}

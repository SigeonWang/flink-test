package com.wxj.steaming.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 20:29
 * @Description: TODO
 */
public class RichMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4);

        /**
         * TODO: RichXxxFunction 富函数
         * 1、比一般的XxxFunction多了生命周期管理方法（每个Function都有对应的RichFunction）
         *      open() 每个子任务启动时调用一次
         *      close() 每个子任务结束时调用一次
         *          异常退出不会调用；
         *          cancel命令退出会正常调用。
         * 2、多了一个运行时上下文 RuntimeContext
         * 3、一般需要任务启动前执行逻辑或执行后清理才会使用，一般用不到！！！
         */
        SingleOutputStreamOperator<Integer> res = ds.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 获取上下文
                RuntimeContext runtimeContext = getRuntimeContext();
                System.out.println("调用open(): 子任务编号 " + runtimeContext.getIndexOfThisSubtask() + "; 子任务名称 " + runtimeContext.getTaskNameWithSubtasks());
            }

            @Override
            public void close() throws Exception {
                super.close();
                RuntimeContext runtimeContext = getRuntimeContext();
                System.out.println("调用close(): 子任务编号 " + runtimeContext.getIndexOfThisSubtask() + "; 子任务名称 " + runtimeContext.getTaskNameWithSubtasks());
            }

            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });

        res.print();

        env.execute();
    }
}

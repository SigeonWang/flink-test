package com.wxj.steaming.state.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author: xingjian wang
 * @Date: 2024/6/4 14:00
 * @Description: 在 map 算子中计算数据的条数
 */
public class ListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.socketTextStream("localhost", 6666)
                .map(new CountMapFunction())
                .print();

        env.execute();
    }

    /**
     * ListState 和 UnionListState 区别：并行度改变时，List中状态分配的方式不同
     *
     * 1）List状态: 合并轮询，均分给新的并行子任务
     * 2）UnionList状态（不推荐）: 原先的多个子任务的状态，台并成一份完整的，然后把完整的列表状态 广播 给新的并行子任务（每个一份完整的）
     *
     * TODO 1、实现 CheckpointedFunction 接口
     *
     */
    public static class CountMapFunction implements MapFunction<String, Long>, CheckpointedFunction {
        private Long count = 0L;
        private ListState<Long> listState;

        @Override
        public Long map(String value) throws Exception {
            return ++ count;  // 注意先加再返回
        }

        /**
         * TODO 2、做 checkpoint 时调用。将 本地变量 持久化到 算子状态 中
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("调用 snapshotState() ...");
            // 2.1 清空算子状态
            listState.clear();
            // 2.2 将本地变量添加到算子状态
            listState.add(count);
        }

        /**
         * TODO 3、程序恢复时调用。初始化本地变量，从状态中把数据添加到本地变量，每个子任务调用一次
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("调用 initializeState() ...");
            // 3.1 获取算子状态
            listState = context
                    .getOperatorStateStore()  // 获取OperatorState存储
                    .getListState(new ListStateDescriptor<Long>("list-state", Types.LONG));  // 获取List状态
//                    .getUnionListState(new ListStateDescriptor<Long>("", Types.LONG));  // 获取UnionList状态
            // 3.2 从算子状态中把数据拷贝到本地变量
            if (context.isRestored()) {
                for (Long cnt : listState.get()) {
                    count += cnt;
                }
            }
        }
    }
}

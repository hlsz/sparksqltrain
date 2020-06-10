package com.data.flink.process.datastream;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

public class FlinkStreamDemo {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    String checkpoint = "";

    public static void main(String[] args) {
        env.enableCheckpointing();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend(checkpint));


    }
}

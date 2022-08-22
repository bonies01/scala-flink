import java.lang
import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
/**
 *
 *算子状态
 * 检查点，状态持久化，状态后端
 */
object Flink8Main {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置检查点
    env.enableCheckpointing(10000)
    val source=env.addSource[Persion1](new MySource1)
    source.addSink(new MySinkFunction)
    env.execute
  }

}

class MySinkFunction extends SinkFunction[Persion1] with CheckpointedFunction{

  override def invoke(value: Persion1, context: SinkFunction.Context): Unit = {

  }

  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {

  }

  override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit = {

  }
}




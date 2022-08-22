import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 间隔join
 *
 */
object Flink6Main {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val s1= env.fromElements(("t1",1000),("t2",2000),("t3",3000))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner[Tuple2[String,Int]]() {
          override def extractTimestamp(t: (String, Int), l: Long): Long = t._2
        }) )
    val s2= env.fromElements(("t1",1100),("t2",2100),("t3",3100))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner[Tuple2[String,Int]]() {
          override def extractTimestamp(t: (String, Int), l: Long): Long = t._2
        }) )
    s1.keyBy(k=>k._1)
      .intervalJoin(s2.keyBy(k=>k._1))
        .between(Time.seconds(1000),Time.seconds(5000))
        .process(new MyProcessJoinFunction).print
    env.execute
  }

}

class MyProcessJoinFunction extends ProcessJoinFunction[Tuple2[String,Int],Tuple2[String,Int],String]{
  var valueState: ValueState[(String, Int)] =null;
  override def open(parameters: Configuration): Unit = {
    //状态失效时间
   val ttlConfig= StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(1))
      .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .build
    val desc: ValueStateDescriptor[(String, Int)] =new ValueStateDescriptor[Tuple2[String,Int]]("value",Types.TUPLE[Tuple2[String,Int]])
      desc.enableTimeToLive(ttlConfig)
    valueState =getRuntimeContext.getState(desc)
  }
  override def processElement(in1: (String, Int), in2: (String, Int), context: ProcessJoinFunction[(String, Int), (String, Int), String]#Context, collector: Collector[String]): Unit = {
    valueState.update(in1)
    collector.collect(in1 +"->"+in2+",valueState"+valueState.value)
  }
}


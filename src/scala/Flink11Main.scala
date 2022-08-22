import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
/**
 *
 *window join 连接
 */
object Flink11Main {
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
    s1.join(s2)
      .where(k=>k._1)
      .equalTo(k=>k._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .apply(new MyApplyFunction).print
    env.execute
  }

}

class MyApplyFunction extends JoinFunction[Tuple2[String,Int],Tuple2[String,Int],String]{

  override def join(in1: (String, Int), in2: (String, Int)): String = {
    in1+"->"+in2
  }
}

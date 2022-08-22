import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{CoMapFunction, CoProcessFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.{Duration, Instant, LocalDateTime, ZoneId}
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *
 * 双流connect
 * 对账
 */
object Flink5Main {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置水位线时间间隔
    env.getConfig.setAutoWatermarkInterval(1000)
    val s1=env.fromElements(("n1",11),("n2",12),("n3",13),("n4",14))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner[Tuple2[String,Int]] {
          override def extractTimestamp(t: (String, Int), l: Long) =t._2
        })
      )
    val s2=env.fromElements(("n1",11,"1"),("n2",12,"2"),("n3",13,"3"),("n4",14,"4"))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner[Tuple3[String,Int,String]] {
          override def extractTimestamp(t: (String, Int, String), l: Long) = t._2
        })
      )
    s1.connect(s2).keyBy(_._1,_._1).process(new MyCoProcessFunction)


    env.execute
  }

}
class MyCoProcessFunction extends CoProcessFunction[Tuple2[String,Int],Tuple3[String,Int,String],String]{

  var state1: ValueState[(String, Int)]=null;
  var state2: ValueState[(String, Int, String)]=null;

  override def open(parameters: Configuration): Unit = {
    state1 = getRuntimeContext.getState(new ValueStateDescriptor[(String,Int)]("st1",Types.TUPLE[(String,Int)]))
     state2 = getRuntimeContext.getState(new ValueStateDescriptor[(String,Int,String)]("st2",Types.TUPLE[(String,Int,String)]))

  }
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[(String, Int), (String, Int, String), String]#OnTimerContext, out: Collector[String]): Unit = {
  if(state1.value!=null){
   out.collect("对账失败"+state1.value)
  }
    if(state2.value!=null){
      out.collect("对账失败"+state2.value)
    }
    state1.clear
    state2.clear
  }

  override def processElement1(in1: (String, Int), context: CoProcessFunction[(String, Int), (String, Int, String), String]#Context, collector: Collector[String]): Unit = {
        //看另一条流中的状态是否来过
      if(state2.value!=null){
        collector.collect("对账成功"+(in1,state2.value))
        //清空状态
        state2.clear
      }
    else{
        //更新状态
        state1.update(in1)
       // 注册一个定时器，开始等待另一条流的状态
        context.timerService().registerEventTimeTimer(in1._2+5000)
      }

  }

  override def processElement2(in2: (String, Int, String), context: CoProcessFunction[(String, Int), (String, Int, String), String]#Context, collector: Collector[String]): Unit = {
    //看另一条流中的状态是否来过
    if(state1.value!=null){
      collector.collect("对账成功"+(in2,state1.value))
      //清空状态
      state1.clear
    }
    else{
      //更新状态
      state2.update(in2)
      // 注册一个定时器，开始等待另一条流的状态
      context.timerService().registerEventTimeTimer(in2._2+5000)
    }
  }
}
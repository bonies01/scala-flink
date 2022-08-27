import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.{PatternProcessFunction, TimedOutPartialMatchHandler}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration

import java.time.{Instant, LocalDateTime, ZoneId}
import java.util
object FlinkCep1Main {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置水位线时间间隔
    env.getConfig.setAutoWatermarkInterval(500)
    val source: DataStream[Persion] = env.addSource(new MySourceFunction)
    val keydStream=source.keyBy(k=>k.name)
    //数据按照顺序一次输入,用状态机处理，状态跳转
    keydStream.flatMap(new StateMachineMapper)
    env.execute
  }
}

class StateMachineMapper extends  RichFlatMapFunction[Persion,String]{
 // var state: ValueState[State]=null;
  override def open(parameters: Configuration): Unit ={

   // state = getRuntimeContext.getState(new ValueStateDescriptor("persionState",State.))
  }
  override def flatMap(in: Persion, collector: Collector[String]): Unit = {

  }
}
class State extends  Enumeration{
  type  State=Value
 val Terminal,Matched=Value
}
case class Transition(event:String,state:State )